import asyncio

import pytest

from broker import broker
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


def test_worker_execute_simple_payload():
    async def _test():
        worker = Worker(worker_id="test-w1")
        res = await worker.execute_task({"id": "t1", "payload": "mensagem teste"})
        assert res == "Processado: mensagem teste"

    asyncio.run(_test())


def test_worker_execute_structured_payload_actions():
    async def _test():
        worker = Worker(worker_id="test-w1")

        # Uppercase
        res_up = await worker.execute_task(
            {"id": "t1", "payload": {"action": "uppercase", "text": "hello world", "duration": 0}}
        )
        assert res_up == {"action": "uppercase", "output": "HELLO WORLD"}

        # Reverse
        res_rev = await worker.execute_task(
            {"id": "t2", "payload": {"action": "reverse", "text": "python", "duration": 0}}
        )
        assert res_rev == {"action": "reverse", "output": "nohtyp"}

        # Sum
        res_sum = await worker.execute_task(
            {"id": "t3", "payload": {"action": "sum", "numbers": [10, 20, 30], "duration": 0}}
        )
        assert res_sum == {"action": "sum", "output": 60}

    asyncio.run(_test())


def test_worker_execute_failure_trigger():
    async def _test():
        worker = Worker(worker_id="test-w1")
        with pytest.raises(RuntimeError, match="Falha de teste"):
            await worker.execute_task(
                {"id": "t-err", "payload": {"fail": True, "error_msg": "Falha de teste"}}
            )

    asyncio.run(_test())


def test_worker_full_cycle_with_broker():
    """
    Testa a integração em tempo de execução entre o Broker e o Worker:
    1. Broker sobe em porta efêmera ou padrão
    2. Tarefa é submetida ao Broker
    3. Worker conecta, processa e envia ACK
    4. Broker confirma conclusão em `broker.done`
    """
    async def _test():
        server = await asyncio.start_server(broker.handle_client, "127.0.0.1", 5001)
        try:
            # Submete tarefa diretamente no broker
            broker.handle_message({"type": "SUBMIT", "task": {"id": "job-100", "payload": "teste-real"}})
            assert len(broker.pending) == 1

            worker = Worker(worker_id="w-integration", host="127.0.0.1", port=5001, poll_interval=0.05)
            # Executa exatamente 1 tarefa e encerra
            await worker.run(max_tasks=1)

            # Verifica se o broker recebeu o ACK e marcou como done
            assert len(broker.pending) == 0
            assert "job-100" in broker.done
            assert broker.done["job-100"] == "Processado: teste-real"
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())


def test_worker_reports_fail_to_broker():
    """
    Testa se uma tarefa com falha envia FAIL ao Broker e este a devolve para a fila pending.
    """
    async def _test():
        server = await asyncio.start_server(broker.handle_client, "127.0.0.1", 5002)
        try:
            broker.handle_message({
                "type": "SUBMIT",
                "task": {
                    "id": "job-fail",
                    "payload": {"fail": True, "error_msg": "Erro fatal de processamento", "duration": 0}
                }
            })
            assert len(broker.pending) == 1

            worker = Worker(worker_id="w-fail-test", host="127.0.0.1", port=5002, poll_interval=0.05)
            await worker.run(max_tasks=1)

            # O broker deve ter recebido FAIL e reinserido a tarefa no pending
            assert len(broker.pending) == 1
            assert broker.pending[0]["id"] == "job-fail"
            assert "job-fail" not in broker.done
        finally:
            server.close()
            await server.wait_closed()

    asyncio.run(_test())
