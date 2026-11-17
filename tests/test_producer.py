import asyncio

import pytest

from broker import broker
from common.protocol import HOST, build_submit, decode, encode
from producer.producer import Producer, ProducerError
from worker.worker import Worker


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


def test_build_submit_with_and_without_task_id():
    assert build_submit("ola") == {"type": "SUBMIT", "task": {"payload": "ola"}}
    assert build_submit("ola", "task-1") == {
        "type": "SUBMIT",
        "task": {"payload": "ola", "id": "task-1"},
    }


def test_submit_returns_task_id_and_adds_task_to_pending():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("ola", task_id="task-1")
            assert task_id == "task-1"
            assert list(broker.pending) == [{"payload": "ola", "id": "task-1"}]
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())


def test_submit_many_sends_tasks_in_order():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            task_ids = await producer.submit_many(["primeira", "segunda", "terceira"])
            assert len(task_ids) == 3
            assert [task["payload"] for task in broker.pending] == [
                "primeira",
                "segunda",
                "terceira",
            ]
            assert [task["id"] for task in broker.pending] == task_ids
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())


def test_invalid_submit_returns_error_and_connection_remains_usable():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection(HOST, port)
            writer.write(encode({"type": "SUBMIT"}))
            await writer.drain()
            missing_response = decode(await reader.readline())
            assert missing_response == {"type": "ERROR", "error": "task inválida"}

            writer.write(encode({"type": "SUBMIT", "task": None}))
            await writer.drain()
            invalid_response = decode(await reader.readline())
            assert invalid_response == {"type": "ERROR", "error": "task inválida"}

            writer.write(encode(build_submit("valida")))
            await writer.drain()
            valid_response = decode(await reader.readline())
            assert valid_response["type"] == "OK"
            assert broker.pending[0]["payload"] == "valida"
            writer.close()
            await writer.wait_closed()
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())


def test_submit_raises_producer_error_when_broker_is_unavailable():
    async def _test():
        producer = Producer(host=HOST, port=0, max_retries=1, reconnect_interval=0.01)
        with pytest.raises(ProducerError):
            await producer.submit("offline")

    asyncio.run(_test())


def test_status_and_result_report_every_task_state():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            await producer.submit("pendente", task_id="pending-task")
            broker.handle_message(
                {"type": "REQUEST_TASK", "worker_id": "worker-1"}
            )
            broker.done["done-task"] = {"output": "ok"}
            broker.failed["failed-task"] = "erro"

            assert await producer.status() == {
                "type": "STATUS",
                "pending": 0,
                "in_progress": 1,
                "done": 1,
                "failed": 1,
            }
            assert (await producer.get_result("pending-task"))["state"] == "in_progress"
            assert (await producer.get_result("done-task")) == {
                "type": "RESULT",
                "task_id": "done-task",
                "state": "done",
                "result": {"output": "ok"},
            }
            assert (await producer.get_result("failed-task"))["result"] == "erro"
            assert (await producer.get_result("missing"))["state"] == "unknown"

            broker.in_progress.clear()
            broker.pending.append({"id": "queued-task", "payload": "queued"})
            assert (await producer.get_result("queued-task"))["state"] == "pending"
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())


def test_wait_result_returns_completed_result():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            broker.done["task-1"] = "resultado pronto"
            producer = Producer(host=HOST, port=port)
            assert await producer.wait_result("task-1", timeout=0.1) == "resultado pronto"
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())


def test_wait_result_raises_on_timeout():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            with pytest.raises(ProducerError, match="Tempo esgotado"):
                await producer.wait_result("unknown", timeout=0.02, poll=0.005)
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())


def test_producer_broker_worker_full_cycle():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("teste do ciclo")
            worker = Worker(
                worker_id="w-producer-integration",
                host=HOST,
                port=port,
                poll_interval=0.01,
            )
            await worker.run(max_tasks=1)
            assert task_id in broker.done
            assert broker.done[task_id] == "Processado: teste do ciclo"
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())
