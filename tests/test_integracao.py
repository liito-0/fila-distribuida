import asyncio
import socket
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

def obter_porta_livre():
    # funcao utilitaria para garantir uma porta livre para testes de reconexao
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((HOST, 0))
    port = s.getsockname()[1]
    s.close()
    return port

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
            
            # 4. Aguarda o Worker capturar no proximo ciclo de polling e finalizar
            await worker_task
            
            assert task_id in broker.done
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 3: Recuperacao de Falha de Processamento Declarada (FAIL) --- #
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
        # Aceleramos as constantes de tempo do broker para nao esperar 30s no teste
        monkeypatch.setattr(broker, "TASK_TIMEOUT_SECONDS", 0.1)
        monkeypatch.setattr(broker, "TIMEOUT_CHECK_INTERVAL_SECONDS", 0.1)
        
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        
        # Inicia o monitor em background (necessario para varrer tarefas expiradas)
        monitor = asyncio.create_task(broker._monitor_timeouts())
        try:
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("tarefa orfa")
            
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

# --- 5: Distribuicao Concorrente sem Duplicacao de Estado --- #
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
            
            # 3. Monitora o Broker ate que as 10 tarefas entrem na lista "done"
            timeout = 3.0
            start = asyncio.get_event_loop().time()
            while len(broker.done) < 10 and (asyncio.get_event_loop().time() - start) < timeout:
                await asyncio.sleep(0.05)
                
            # 4. Para os workers graciosamente usando o metodo interno deles
            for w in workers:
                w.stop()
            await asyncio.gather(*worker_tasks, return_exceptions=True)
            
            # 5. Validacao: A fila deve estar vazia e todas as 10 tarefas concluidas corretamente
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

# --- 6: Garantia de Processamento Sequencial (FIFO) --- #
def test_integracao_garantia_de_ordem_fifo():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            # Envia 5 tarefas usando o recurso de lote do Producer
            task_ids = await producer.submit_many([f"T-{i}" for i in range(5)])
            
            worker = Worker(worker_id="w-fifo", host=HOST, port=port, poll_interval=0.01)
            await worker.run(max_tasks=5)
            
            # Validacao: No Python 3.7+, dicionarios mantem a ordem de insercao.
            # Logo, as chaves em broker.done devem estar na mesma ordem de task_ids.
            chaves_concluidas = list(broker.done.keys())
            assert chaves_concluidas == task_ids
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 7: Prioridade de Tarefas Re-enfileiradas (Timeout fura a fila) --- #
def test_integracao_prioridade_timeout_extendleft(monkeypatch):
    async def _test():
        monkeypatch.setattr(broker, "TASK_TIMEOUT_SECONDS", 0.1)
        monkeypatch.setattr(broker, "TIMEOUT_CHECK_INTERVAL_SECONDS", 0.05)
        
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            
            task_id_1 = await producer.submit("urgente")
            reader, writer = await asyncio.open_connection(HOST, port)
            writer.write(encode({"type": "REQUEST_TASK", "worker_id": "fantasma"}))
            await writer.drain()
            await reader.readline()
            writer.close()
            await writer.wait_closed()
            
            task_id_2 = await producer.submit("normal")
            
            broker.in_progress[task_id_1]["started_at"] = 0
            broker._requeue_expired_tasks(now=100.0)
            
            assert [t["id"] for t in broker.pending] == [task_id_1, task_id_2]
            
            worker = Worker(worker_id="w-prioridade", host=HOST, port=port, poll_interval=0.01)
            await worker.run(max_tasks=2)
            
            chaves_concluidas = list(broker.done.keys())
            assert chaves_concluidas == [task_id_1, task_id_2]
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 8: Resiliencia do Worker com Broker Atrasado (Reconexao) --- #
def test_integracao_worker_inicia_antes_do_broker():
    async def _test():
        porta_livre = obter_porta_livre()
        
        # Inicia o worker apontando para uma porta onde AINDA NAO EXISTE um broker
        worker = Worker(worker_id="w-sobrevivente", host=HOST, port=porta_livre, reconnect_interval=0.1, poll_interval=0.05)
        worker_task = asyncio.create_task(worker.run(max_tasks=1))
        
        # Da um tempinho para o worker falhar e entrar no sleep de reconexao
        await asyncio.sleep(0.2)
        
        # Agora sim subimos o broker na mesma porta
        server = await asyncio.start_server(broker.handle_client, HOST, porta_livre)
        try:
            producer = Producer(host=HOST, port=porta_livre)
            task_id = await producer.submit("sobreviveu")
            
            # O worker deve se recuperar da falha de conexao sozinho, pedir a tarefa e terminar
            await asyncio.wait_for(worker_task, timeout=2.0)
            
            assert broker.done[task_id] == "Processado: sobreviveu"
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 9: Protecao contra Lixo na Rede (Mensagens Malformadas) --- #
def test_integracao_broker_nao_cai_com_lixo_na_rede():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            reader, writer = await asyncio.open_connection(HOST, port)
            
            # 1. Envia um JSON quebrado
            writer.write(b"ISSO NAO E UM JSON\n")
            await writer.drain()
            resp_invalida = decode(await reader.readline())
            assert resp_invalida["type"] == "ERROR"
            
            # 2. Envia um comando que o broker nao conhece
            writer.write(encode({"type": "DESTRUIR_FILA"}))
            await writer.drain()
            resp_desconhecida = decode(await reader.readline())
            assert resp_desconhecida["type"] == "ERROR"
            assert "tipo desconhecido" in resp_desconhecida["error"]
            writer.close()
            
            # 3. Prova que o broker continua funcionando apos os ataques enviando uma tarefa normal
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("to vivo")
            assert len(broker.pending) == 1
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 10: Processamento de Acoes Estruturadas (Payload Complexo) --- #
def test_integracao_acoes_estruturadas():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            
            # Monta pacotes baseados nas funcionalidades mapeadas no worker
            id_sum = await producer.submit({"action": "sum", "numbers": [10, 5, 2], "duration": 0})
            id_up = await producer.submit({"action": "uppercase", "text": "distribuido", "duration": 0})
            id_rev = await producer.submit({"action": "reverse", "text": "roma", "duration": 0})
            
            worker = Worker(worker_id="w-estruturado", host=HOST, port=port, poll_interval=0.01)
            await worker.run(max_tasks=3)
            
            # Valida os processamentos feitos no worker sobre a rede
            assert broker.done[id_sum]["output"] == 17
            assert broker.done[id_up]["output"] == "DISTRIBUIDO"
            assert broker.done[id_rev]["output"] == "amor"
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())
