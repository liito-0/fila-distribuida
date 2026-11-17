import asyncio

import pytest

from broker import broker
from common.protocol import decode, encode


@pytest.fixture(autouse=True)
def clear_broker_state():
    broker.pending.clear()
    broker.in_progress.clear()
    broker.done.clear()
    broker.failed.clear()
    broker.attempts.clear()
    yield
    broker.pending.clear()
    broker.in_progress.clear()
    broker.done.clear()
    broker.failed.clear()
    broker.attempts.clear()


def _submit_and_assign(task_id):
    broker.handle_message({"type": "SUBMIT", "task": {"id": task_id}})
    broker.handle_message({"type": "REQUEST_TASK", "worker_id": "worker-1"})


def test_expired_task_returns_to_front_of_queue_and_logs_warning(caplog):
    _submit_and_assign("expired")
    broker.handle_message({"type": "SUBMIT", "task": {"id": "waiting"}})
    broker.in_progress["expired"]["started_at"] = 100

    broker._requeue_expired_tasks(now=130.001)

    assert [task["id"] for task in broker.pending] == ["expired", "waiting"]
    assert "expired" not in broker.in_progress
    assert "Tarefa expired expirou" in caplog.text


def test_task_is_not_requeued_until_more_than_30_seconds():
    _submit_and_assign("task-1")
    broker.in_progress["task-1"]["started_at"] = 100

    broker._requeue_expired_tasks(now=130)

    assert "task-1" in broker.in_progress
    assert not broker.pending

    broker._requeue_expired_tasks(now=130.001)

    assert "task-1" not in broker.in_progress
    assert broker.pending[0]["id"] == "task-1"


def test_expired_tasks_keep_their_order_at_front_of_queue():
    _submit_and_assign("task-1")
    _submit_and_assign("task-2")
    broker.handle_message({"type": "SUBMIT", "task": {"id": "waiting"}})
    broker.in_progress["task-1"]["started_at"] = 100
    broker.in_progress["task-2"]["started_at"] = 100

    broker._requeue_expired_tasks(now=131)

    assert [task["id"] for task in broker.pending] == [
        "task-1",
        "task-2",
        "waiting",
    ]


def test_acknowledged_task_is_not_requeued():
    _submit_and_assign("task-1")
    broker.handle_message(
        {"type": "ACK", "task_id": "task-1", "result": "finished"}
    )

    broker._requeue_expired_tasks(now=float("inf"))

    assert not broker.pending
    assert broker.done["task-1"] == "finished"


def test_failed_task_is_not_requeued_again_by_timeout():
    _submit_and_assign("task-1")
    broker.in_progress["task-1"]["started_at"] = 100
    broker.handle_message(
        {"type": "FAIL", "task_id": "task-1", "error": "failed"}
    )

    broker._requeue_expired_tasks(now=131)

    assert [task["id"] for task in broker.pending] == ["task-1"]
    assert "task-1" not in broker.in_progress


def test_late_ack_after_timeout_is_ignored(caplog):
    _submit_and_assign("task-1")
    broker.in_progress["task-1"]["started_at"] = 100
    broker._requeue_expired_tasks(now=131)

    reply = broker.handle_message(
        {"type": "ACK", "task_id": "task-1", "result": "late"}
    )

    assert reply == {"type": "OK"}
    assert [task["id"] for task in broker.pending] == ["task-1"]
    assert "task-1" not in broker.done
    assert "ACK ignorado" in caplog.text


def test_unknown_fail_is_ignored(caplog):
    reply = broker.handle_message(
        {"type": "FAIL", "task_id": "unknown", "error": "failed"}
    )

    assert reply == {"type": "OK"}
    assert not broker.pending
    assert "unknown" not in broker.in_progress
    assert "FAIL ignorado" in caplog.text


def test_task_is_marked_failed_after_max_failures():
    broker.handle_message({"type": "SUBMIT", "task": {"id": "task-1"}})

    for attempt in range(broker.MAX_ATTEMPTS):
        broker.handle_message({"type": "REQUEST_TASK", "worker_id": "worker-1"})
        broker.handle_message(
            {"type": "FAIL", "task_id": "task-1", "error": f"failure-{attempt + 1}"}
        )

    assert broker.attempts["task-1"] == broker.MAX_ATTEMPTS
    assert broker.failed["task-1"] == "failure-3"
    assert not broker.pending
    assert "task-1" not in broker.in_progress


def test_task_is_marked_failed_after_max_timeouts():
    broker.handle_message({"type": "SUBMIT", "task": {"id": "task-1"}})

    for _ in range(broker.MAX_ATTEMPTS):
        broker.handle_message({"type": "REQUEST_TASK", "worker_id": "worker-1"})
        broker.in_progress["task-1"]["started_at"] = 100
        broker._requeue_expired_tasks(now=131)

    assert broker.attempts["task-1"] == broker.MAX_ATTEMPTS
    assert broker.failed["task-1"] == "timeout"
    assert not broker.pending
    assert "task-1" not in broker.in_progress


def test_worker_disconnect_requeues_unacknowledged_task():
    async def run_server():
        broker.handle_message({"type": "SUBMIT", "task": {"id": "task-1"}})
        server = await asyncio.start_server(
            broker.handle_client, "127.0.0.1", 0
        )
        try:
            port = server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1", port)
            writer.write(
                encode({"type": "REQUEST_TASK", "worker_id": "worker-1"})
            )
            await writer.drain()
            response = decode(await reader.readline())
            assert response["type"] == "TASK"

            writer.close()
            await writer.wait_closed()
            for _ in range(100):
                if broker.pending:
                    break
                await asyncio.sleep(0.001)
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(run_server())

    assert [task["id"] for task in broker.pending] == ["task-1"]
    assert "task-1" not in broker.in_progress


def test_background_monitor_requeues_expired_task(monkeypatch):
    async def run_monitor():
        monkeypatch.setattr(broker, "TASK_TIMEOUT_SECONDS", 0)
        monkeypatch.setattr(broker, "TIMEOUT_CHECK_INTERVAL_SECONDS", 0.001)
        _submit_and_assign("task-1")
        broker.in_progress["task-1"]["started_at"] -= 1

        monitor = asyncio.create_task(broker._monitor_timeouts())
        try:
            for _ in range(100):
                if broker.pending:
                    break
                await asyncio.sleep(0.001)
        finally:
            monitor.cancel()
            await asyncio.gather(monitor, return_exceptions=True)

        assert broker.pending[0]["id"] == "task-1"
        assert "task-1" not in broker.in_progress

    asyncio.run(run_monitor())
