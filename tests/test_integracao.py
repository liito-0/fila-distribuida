import asyncio
import socket
import pytest

from broker import broker
from common.protocol import HOST, encode, decode
from producer.producer import Producer, ProducerError
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
            
            task_id_2 = await producer.submit("normal")
            
            broker.in_progress[task_id_1]["started_at"] = 0
            broker._requeue_expired_tasks(now=100.0)
            
            assert [t["id"] for t in broker.pending] == [task_id_1, task_id_2]
            
            worker = Worker(worker_id="w-prioridade", host=HOST, port=port, poll_interval=0.01)
            await worker.run(max_tasks=2)
            
            chaves_concluidas = list(broker.done.keys())
            assert chaves_concluidas == [task_id_1, task_id_2]
            writer.close()
            await writer.wait_closed()
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

# --- 11: Task_id Duplicado --- #
def test_integracao_submissao_task_id_duplicado():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)

            # Submetendo a primeira tarefa com id customizado
            task_id = await producer.submit("primeira tarefa", task_id="id-duplicado")
            assert task_id == "id-duplicado"
            assert len(broker.pending) == 1

            # Tentando submeter segunda tarefa com o mesmo task_id
            with pytest.raises(ProducerError):
                await producer.submit("segunda tarefa", task_id="id-duplicado")

            # Verificando se apenas a primeira tarefa permaneceu na fila
            assert len(broker.pending) == 1

            # Worker processa a tarefa original sem sobrescrita de resultado
            worker = Worker(worker_id="w-dup", host=HOST, port=port, poll_interval=0.01)
            await worker.run(max_tasks=1)

            assert broker.done["id-duplicado"] == "Processado: primeira tarefa"
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 12: Verificando o fluxo pós falha (Reinserção acontecendo corretamente) --- #
def test_integracao_pos_falha():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("tarefa a ser recuperada")

            # Worker pegar a tarefa mas deve dar timeout antes do ACK 
            reader, writer = await asyncio.open_connection(HOST, port)
            writer.write(encode({"type": "REQUEST_TASK", "worker_id": "worker-1"}))
            await writer.drain()

            resposta = decode(await reader.readline())
            assert resposta["type"] == "TASK"
            assert task_id in broker.in_progress

            writer.close()
            await writer.wait_closed()

            # Simulando o timeout 
            broker.in_progress[task_id]["started_at"] = 0
            broker._requeue_expired_tasks(now=100)

            # Verificando se a task esta no broker ou nao 
            assert task_id not in broker.in_progress
            assert len(broker.pending) == 1 
            assert broker.pending[0]["id"] == task_id
            
            #Worker 2 deve assumir a tarefa reinserida para concluir o fluxo 
            worker_2 = Worker(worker_id="worker-2", host=HOST, port=port, poll_interval=0.01)
            await worker_2.run(max_tasks=1)

            # Verificando se a tarefa foi recuperada e concluida 
            assert len(broker.pending) == 0
            assert len(broker.in_progress) == 0
            assert task_id in broker.done 
            assert broker.done[task_id] == "Processado: tarefa a ser recuperada"
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 13: Concorrencia M:N (Multiplos Producers e Multiplos Workers) --- #
def test_integracao_concorrencia_multiplos_producers_e_workers():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            # Iniciando 3 workers concorrentes antes dos envios terminarem
            workers = [
                Worker(worker_id=f"w-mn-{i}", host=HOST, port=port, poll_interval=0.02)
                for i in range(3)
            ]
            worker_tasks = [asyncio.create_task(w.run()) for w in workers]

            # Disparando 4 producers simultaneos enviando 5 tarefas cada
            async def produzir_lote(prod_idx):
                prod = Producer(producer_id=f"p-mn-{prod_idx}", host=HOST, port=port)
                return await prod.submit_many([f"P{prod_idx}-T{t}" for t in range(5)])

            resultados_envio = await asyncio.gather(*(produzir_lote(i) for i in range(4)))
            todos_ids = [tid for lote in resultados_envio for tid in lote]

            # Aguardando todas as 20 tarefas serem processadas
            timeout = 4.0
            inicio = asyncio.get_event_loop().time()
            while len(broker.done) < 20 and (asyncio.get_event_loop().time() - inicio) < timeout:
                await asyncio.sleep(0.05)

            # Encerrando os workers
            for w in workers:
                w.stop()
            await asyncio.gather(*worker_tasks, return_exceptions=True)

            # Verificando se nenhuma tarefa foi perdida ou duplicada
            assert len(todos_ids) == 20
            assert len(broker.done) == 20
            assert len(broker.pending) == 0
            assert len(broker.in_progress) == 0
            for tid in todos_ids:
                assert tid in broker.done
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 14: Queda e Reinicio do Broker em Tempo de Execucao --- #
def test_integracao_queda_e_reinicio_broker_mid_flight():
    async def _test():
        porta = obter_porta_livre()
        conexoes_ativas = set()

        async def handle_rastreado(reader, writer):
            conexoes_ativas.add(writer)
            try:
                await broker.handle_client(reader, writer)
            finally:
                conexoes_ativas.discard(writer)

        # Subindo o broker inicial e conectando um worker em polling
        server_1 = await asyncio.start_server(handle_rastreado, HOST, porta)
        worker = Worker(
            worker_id="w-resiliente",
            host=HOST,
            port=porta,
            poll_interval=0.05,
            reconnect_interval=0.1,
        )
        worker_task = asyncio.create_task(worker.run(max_tasks=1))
        await asyncio.sleep(0.15)

        # Simulando queda abrupta do broker e fechando sockets ativos
        server_1.close()
        for conn in list(conexoes_ativas):
            conn.close()
        await server_1.wait_closed()

        # Aguardando o worker detectar a queda e entrar em reconexao
        await asyncio.sleep(0.2)

        # Reiniciando o broker na mesma porta e submetendo nova tarefa
        server_2 = await asyncio.start_server(broker.handle_client, HOST, porta)
        try:
            producer = Producer(host=HOST, port=porta)
            task_id = await producer.submit("apos reinicio")

            await asyncio.wait_for(worker_task, timeout=2.0)

            # Verificando se o worker reconectou e concluiu a tarefa
            assert task_id in broker.done
            assert broker.done[task_id] == "Processado: apos reinicio"
        finally:
            server_2.close()
            await server_2.wait_closed()
    asyncio.run(_test())

# --- 15: Recuperacao Pos-FAIL Transitorio e Ordem no Fim da Fila --- #
def test_integracao_pos_fail_transitorio_e_ordem_fila():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        try:
            # Submetendo duas tarefas na fila
            producer = Producer(host=HOST, port=port)
            task_id_1 = await producer.submit("tarefa-instavel")
            task_id_2 = await producer.submit("tarefa-estavel")

            # Worker 1 sofre erro transitorio na primeira tarefa e envia FAIL
            worker_1 = Worker(worker_id="w-falha", host=HOST, port=port, poll_interval=0.01)

            async def erro_transitorio(task):
                raise RuntimeError("Falha temporaria de rede/disco")

            worker_1.execute_task = erro_transitorio
            await worker_1.run(max_tasks=1)

            # Verificando se a tarefa que falhou foi para o fim da fila
            assert [t["id"] for t in broker.pending] == [task_id_2, task_id_1]

            # Worker 2 saudavel assume a fila na nova ordem e conclui ambas
            worker_2 = Worker(worker_id="w-saudavel", host=HOST, port=port, poll_interval=0.01)
            await worker_2.run(max_tasks=2)

            # Verificando se ambas as tarefas foram concluidas na ordem esperada
            assert len(broker.pending) == 0
            assert len(broker.in_progress) == 0
            assert list(broker.done.keys()) == [task_id_2, task_id_1]
            assert broker.done[task_id_1] == "Processado: tarefa-instavel"
        finally:
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 16: Condicao de Corrida (FAIL Atrasado Apos Outro Worker Concluir) --- #
def test_integracao_fail_atrasado_apos_conclusao_por_outro_worker():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        w_lento = None
        try:
            # Submetendo tarefa disputada
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit("tarefa disputada")

            # Worker lento pega a tarefa
            r_lento, w_lento = await asyncio.open_connection(HOST, port)
            w_lento.write(encode({"type": "REQUEST_TASK", "worker_id": "worker-lento"}))
            await w_lento.drain()
            resp_task = decode(await r_lento.readline())
            assert resp_task["type"] == "TASK"

            # Simulando o timeout para a tarefa voltar para pending
            broker.in_progress[task_id]["started_at"] = 0
            broker._requeue_expired_tasks(now=100.0)

            # Worker rapido assume a tarefa e conclui antes do lento responder
            worker_rapido = Worker(worker_id="worker-rapido", host=HOST, port=port, poll_interval=0.01)
            await worker_rapido.run(max_tasks=1)
            assert task_id in broker.done

            # Worker lento acorda tarde e envia FAIL para a tarefa ja concluida
            w_lento.write(encode({"type": "FAIL", "task_id": task_id,"worker_id": "worker_lento", "error": "falha tardia"}))
            await w_lento.drain()
            resp_fail = decode(await r_lento.readline())
            assert resp_fail["type"] == "ERROR"

            # Verificando se o broker ignorou o FAIL tardio e manteve a tarefa concluida
            assert len(broker.pending) == 0
            assert len(broker.in_progress) == 0
            assert broker.done[task_id] == "Processado: tarefa disputada"
        finally:
            if w_lento is not None:
                w_lento.close()
                await w_lento.wait_closed()
            server.close()
            await server.wait_closed()
    asyncio.run(_test())

# --- 17: FAIL Tardio Durante Execucao de Outro Worker --- #
def test_integracao_fail_tardio_durante_execucao_de_outro_worker():
    async def _test():
        server = await asyncio.start_server(broker.handle_client, HOST, 0)
        port = server.sockets[0].getsockname()[1]
        w_lento = None
        try:
            # Submetendo tarefa com duracao suficiente para outro worker ainda estar rodando
            producer = Producer(host=HOST, port=port)
            task_id = await producer.submit({"action": "uppercase", "text": "concorrencia", "duration": 0.4})

            # Worker lento pega a tarefa
            r_lento, w_lento = await asyncio.open_connection(HOST, port)
            w_lento.write(encode({"type": "REQUEST_TASK", "worker_id": "worker-lento"}))
            await w_lento.drain()
            resp_task = decode(await r_lento.readline())
            assert resp_task["type"] == "TASK"

            # Simulando o timeout para a tarefa voltar para pending
            broker.in_progress[task_id]["started_at"] = 0
            broker._requeue_expired_tasks(now=100.0)

            # Worker rapido inicia em background e assume a tarefa (ainda em execucao)
            worker_rapido = Worker(worker_id="worker-rapido", host=HOST, port=port, poll_interval=0.01)
            task_rapido = asyncio.create_task(worker_rapido.run(max_tasks=1))
            await asyncio.sleep(0.05)
            assert broker.in_progress[task_id]["worker"] == "worker-rapido"

            # Worker lento acorda antes do rapido terminar e envia FAIL
            w_lento.write(encode({"type": "FAIL", "task_id": task_id,"worker_id": "worker-lento", "error": "falha tardia"}))
            await w_lento.drain()
            await r_lento.readline()

            # Validacao: a tarefa NAO deveria voltar para pending enquanto worker-rapido executa
            assert len(broker.pending) == 0
            assert task_id in broker.in_progress

            await task_rapido
        finally:
            if w_lento is not None:
                w_lento.close()
                await w_lento.wait_closed()
            server.close()
            await server.wait_closed()
    asyncio.run(_test())
