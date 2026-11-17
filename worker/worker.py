import argparse
import asyncio
from contextlib import suppress
import logging
import os
from pathlib import Path
import sys
import time
from typing import Any
import uuid

# Permite execução tanto via `python worker/worker.py` quanto `python -m worker.worker`
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.protocol import HOST, PORT, decode, encode


class Worker:
    """
    Worker para consumo e execução de tarefas de uma fila distribuída.
    
    Conecta-se ao Broker, solicita tarefas pendentes, executa-as
    e envia a confirmação (ACK) ou sinalização de erro (FAIL).
    """

    def __init__(
        self,
        worker_id: str | None = None,
        host: str = HOST,
        port: int = PORT,
        poll_interval: float = 1.0,
        reconnect_interval: float = 3.0,
    ):
        self.worker_id = worker_id or f"worker-{uuid.uuid4().hex[:6]}"
        self.host = host
        self.port = port
        self.poll_interval = poll_interval
        self.reconnect_interval = reconnect_interval
        self._running = False

        self.logger = logging.getLogger(f"Worker-{self.worker_id}")

    async def _send_and_receive(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, msg: dict
    ) -> dict:
        """Envia mensagem serializada e aguarda a resposta do Broker."""
        writer.write(encode(msg))
        await writer.drain()

        line = await reader.readline()
        if not line:
            raise ConnectionResetError("Conexão fechada pelo Broker.")
        return decode(line)

    async def request_task(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> dict:
        """Solicita uma tarefa ao Broker."""
        msg = {"type": "REQUEST_TASK", "worker_id": self.worker_id}
        return await self._send_and_receive(reader, writer, msg)

    async def send_ack(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        task_id: str,
        result: Any,
    ) -> dict:
        """Confirma a conclusão com sucesso de uma tarefa (ACK)."""
        msg = {"type": "ACK", "task_id": task_id,"worker_id": self.worker_id, "result": result}
        return await self._send_and_receive(reader, writer, msg)

    async def send_fail(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        task_id: str,
        error: str,
    ) -> dict:
        """Notifica o Broker sobre falha no processamento da tarefa (FAIL)."""
        msg = {"type": "FAIL", "task_id": task_id, "worker_id": self.worker_id, "error": error}
        return await self._send_and_receive(reader, writer, msg)

    async def execute_task(self, task: dict) -> Any:
        """
        Executa a tarefa recebida.
        
        Pode receber payloads em formato string, número ou dicionário
        com ações específicas (ex: "uppercase", "reverse", "sum", "sleep").
        Se o payload contiver 'fail': True, simula propositalmente uma falha.
        """
        payload = task.get("payload")
        task_id = task.get("id", "sem-id")

        self.logger.info("Executando tarefa %s: %s", task_id, payload)

        # Suporte a payload estruturado (dicionário)
        if isinstance(payload, dict):
            # Simulação de erro controlada (útil para testes de tolerância a falhas)
            if payload.get("fail") is True:
                erro = payload.get("error_msg", "Erro simulado na execução da tarefa")
                raise RuntimeError(erro)

            # Simulação de tarefa demorada
            duration = payload.get("duration", 0.5)
            if duration > 0:
                await asyncio.sleep(duration)

            action = payload.get("action")
            if action == "uppercase":
                texto = payload.get("text", "")
                return {"action": action, "output": str(texto).upper()}
            if action == "reverse":
                texto = payload.get("text", "")
                return {"action": action, "output": str(texto)[::-1]}
            if action == "sum":
                nums = payload.get("numbers", [])
                return {"action": action, "output": sum(nums)}

            return {"status": "ok", "processed": payload}

        # Payload simples (string, número, etc.)
        await asyncio.sleep(0.5)  # simulação de processamento
        if isinstance(payload, str):
            return f"Processado: {payload}"

        return {"status": "ok", "result": payload}

    async def run(self, max_tasks: int | None = None) -> None:
        """
        Ciclo principal do worker:
        - Conecta ao broker
        - Pede tarefas em loop
        - Executa e envia ACK/FAIL
        - Reconecta automaticamente se a conexão for perdida
        """
        self._running = True
        tasks_completed = 0

        self.logger.info(
            "Worker iniciado (ID: %s). Broker alvo: %s:%s",
            self.worker_id,
            self.host,
            self.port,
        )

        while self._running:
            reader = None
            writer = None
            try:
                self.logger.info("Conectando ao Broker em %s:%s...", self.host, self.port)
                reader, writer = await asyncio.open_connection(self.host, self.port)
                self.logger.info("Conexão estabelecida com sucesso!")

                while self._running:
                    # 1. Solicita tarefa
                    response = await self.request_task(reader, writer)
                    resp_type = response.get("type")

                    if resp_type == "TASK":
                        task = response["task"]
                        task_id = task["id"]
                        self.logger.info("Tarefa recebida [%s]", task_id)

                        # 2. Executa a tarefa capturando sucessos ou exceções
                        try:
                            result = await self.execute_task(task)
                            self.logger.info("Tarefa [%s] finalizada com sucesso.", task_id)
                            ack_res = await self.send_ack(reader, writer, task_id, result)
                            self.logger.debug("Confirmação do ACK recebida: %s", ack_res)
                        except Exception as e:
                            self.logger.error("Falha ao processar tarefa [%s]: %s", task_id, e)
                            fail_res = await self.send_fail(reader, writer, task_id, str(e))
                            self.logger.debug("Confirmação do FAIL recebida: %s", fail_res)

                        tasks_completed += 1
                        if max_tasks is not None and tasks_completed >= max_tasks:
                            self.logger.info(
                                "Meta de %d tarefas atingida. Encerrando worker.",
                                max_tasks,
                            )
                            self._running = False
                            break

                    elif resp_type == "NO_TASK":
                        self.logger.debug(
                            "Fila vazia. Aguardando %ss para nova tentativa...",
                            self.poll_interval,
                        )
                        if max_tasks is not None and tasks_completed >= max_tasks:
                            self._running = False
                            break
                        await asyncio.sleep(self.poll_interval)

                    else:
                        self.logger.warning("Resposta inesperada do broker: %s", response)
                        await asyncio.sleep(self.poll_interval)

            except (ConnectionRefusedError, OSError, ConnectionResetError) as e:
                self.logger.warning(
                    "Não foi possível comunicar com o Broker (%s). Tentando reconectar em %ss...",
                    e,
                    self.reconnect_interval,
                )
                await asyncio.sleep(self.reconnect_interval)
            except asyncio.CancelledError:
                self.logger.info("Encerrando execução do worker...")
                self._running = False
                break
            finally:
                if writer:
                    writer.close()
                    with suppress(Exception):
                        await writer.wait_closed()

        self.logger.info("Worker finalizado.")

    def stop(self) -> None:
        """Sinaliza parada para o worker."""
        self._running = False


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Worker da Fila Distribuída de Tarefas")
    parser.add_argument("--id", dest="worker_id", default=None, help="Identificador do worker (ex: worker-1)")
    parser.add_argument("--host", default=HOST, help=f"Endereço do Broker (padrão: {HOST})")
    parser.add_argument("--port", type=int, default=PORT, help=f"Porta do Broker (padrão: {PORT})")
    parser.add_argument("--poll-interval", type=float, default=1.0, help="Intervalo de espera quando fila vazia (s)")
    parser.add_argument("--max-tasks", type=int, default=None, help="Executar no máximo N tarefas e finalizar")
    parser.add_argument("--debug", action="store_true", help="Habilitar logs de debug")

    args = parser.parse_args()

    setup_logging(level=logging.DEBUG if args.debug else logging.INFO)

    worker = Worker(
        worker_id=args.worker_id,
        host=args.host,
        port=args.port,
        poll_interval=args.poll_interval,
    )

    try:
        asyncio.run(worker.run(max_tasks=args.max_tasks))
    except KeyboardInterrupt:
        print("\n[Worker] Interrompido pelo usuário (Ctrl+C). Finalizando...")


if __name__ == "__main__":
    main()
