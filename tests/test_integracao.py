import asyncio
import pytest

from broker import broker
from common.protocol import HOST, encode, decode
from producer.producer import Producer
from worker.worker import Worker

@pytest.fixture(autouse=True)
def clear_broker_state():
    # garante que a fila esteja limpa antes e depois de cada teste
    broker.pending.clear()
    broker.in_progress.clear()
    broker.done.clear()
    yield
    broker.pending.clear()
    broker.in_progress.clear()
    broker.done.clear()

# --- 1: Fluxo de Sucesso (Ponta a Ponta) --- #
def test_integracao_fluxo_sucesso_ponta_a_ponta():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            # 1. Producer envia
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("teste de integracao E2E")
            
            # 2. Worker processa 1 tarefa e encerra
            worker = Worker(worker_id="w-int-1", host=HOST, port=port, poll_interval=0.01)
            await worker.run(max_tasks=1)
            
            # 3. Validacao do estado no broker
            assert task_id in broker.done
            assert broker.done[task_id] == "Processado: teste de integracao E2E"
            assert len(broker.pending) == 0
            assert len(broker.in_progress) == 0
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 2: Comportamento de Fila Vazia (Polling Continuo) --- #
def test_integracao_fila_vazia_polling():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            # 1. Inicia o Worker SEM tarefas na fila
            worker = Worker(worker_id="w-int-2", host=HOST, port=port, poll_interval=0.1)
            worker_task = asyncio.create_task(worker.run(max_tasks=1))
            
            # 2. Aguarda um pouco para o Worker receber NO_TASK e entrar no sleep
            await asyncio.sleep(0.3)
            assert len(broker.pending) == 0
            
            # 3. Producer submete uma tarefa com atraso
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("tarefa atrasada")
            
            # 4. Aguarda o Worker capturar no próximo ciclo de polling e finalizar
            await worker_task
            
            assert task_id in broker.done
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 3. Recuperacao de Falha de Processamento Declarada (FAIL) --- #
def test_integracao_falha_de_processamento():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            # Payload configurado para falhar de proposito
            payload = {"fail": True, "error_msg": "Erro fatal simulado"}
            task_id = await producer.submit(payload)
            
            worker = Worker(worker_id="w-int-3", host=HOST, port=port, poll_interval=0.01)
            await worker.run(max_tasks=1)
            
            # Validacao: A tarefa deve ter voltado para 'pending' devido ao FAIL
            assert len(broker.pending) == 1
            assert broker.pending[0]["id"] == task_id
            assert task_id not in broker.done
            assert task_id not in broker.in_progress
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 4: Tratamento de Tarefas Expiradas por Queda do Worker (Timeout) --- #
def test_integracao_timeout_worker_fantasma(monkeypatch):
    async def _test():
        # Aceleramos as constantes de tempo do broker para não esperar 30s no teste
        monkeypatch.setattr(broker, "TASK_TIMEOUT_SECONDS", 0.1)
        monkeypatch.setattr(broker, "TIMEOUT_CHECK_INTERVAL_SECONDS", 0.1)
        
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        
        # Inicia o monitor em background (necessario para varrer tarefas expiradas)
        monitor = asyncio.create_task(broker._monitor_timeouts())
        try:
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("tarefa órfã")
            
            # Simula um worker que pede a tarefa mas e desligado/perde internet antes do ACK
            reader, writer = await asyncio.open_connection(HOST, port)
            writer.write(encode({"type": "REQUEST_TASK", "worker_id": "worker-morto"}))
            await writer.drain()
            resp = decode(await reader.readline())
            
            assert resp["type"] == "TASK"
            assert task_id in broker.in_progress
            
            # Corta a conexao (Worker "caiu")
            writer.close()
            await writer.wait_closed()
            
            # Aguarda tempo suficiente para o monitor de timeout atuar
            await asyncio.sleep(0.3)
            
            # Validacao: O broker deve ter reconhecido a queda e devolvido para pending
            assert task_id not in broker.in_progress
            assert len(broker.pending) == 1
            assert broker.pending[0]["id"] == task_id
        finally:
            monitor.cancel()
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 5: Distribuição Concorrente sem Duplicação de Estado ---
def test_integracao_distribuicao_concorrente():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            # 1. Producer envia 10 tarefas em lote
            producer = Producer(host=HOST, port=port)
            task_ids = await producer.submit_many([f"Carga_{i}" for i in range(10)])
            
            # 2. Inicia 3 workers rodando simultaneamente sem limite de tarefas
            workers = [Worker(worker_id=f"w-int-conc-{i}", host=HOST, port=port, poll_interval=0.1) for i in range(3)]
            worker_tasks = [asyncio.create_task(w.run()) for w in workers]
            
            # 3. Monitora o Broker até que as 10 tarefas entrem na lista "done"
            timeout = 3.0
            start = asyncio.get_event_loop().time()
            while len(broker.done) < 10 and (asyncio.get_event_loop().time() - start) < timeout:
                await asyncio.sleep(0.05)
                
            # 4. Para os workers graciosamente usando o método interno deles
            for w in workers:
                w.stop()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            
            # 5. Validação: A fila deve estar vazia e todas as 10 tarefas concluídas corretamente
            assert len(broker.done) == 10
            assert len(broker.pending) == 0
            assert len(broker.in_progress) == 0
            
            # Confirma que exatamente as 10 IDs submetidas estao presentes
            for tid in task_ids:
                assert tid in broker.done
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())
