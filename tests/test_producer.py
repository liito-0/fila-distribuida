import asyncio

import pytest

from broker import broker
from common.protocol import HOST, PORT, build_submit, decode, encode
from producer.producer import Producer, ProducerError
from worker.worker import Worker


@pytest.fixture(autouse=True)
def clear_broker_state():
    broker.pending.clear()
    broker.in_progress.clear()
    broker.done.clear()
    yield
    broker.pending.clear()
    broker.in_progress.clear()
    broker.done.clear()


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
