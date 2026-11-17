"""Cliente produtor para a fila distribuída."""

import argparse
import asyncio
import logging
from pathlib import Path
import sys
import time
import uuid
from typing import Any

# Permite execução via `python producer/producer.py` e `python -m producer.producer`.
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from common.protocol import (  # noqa: E402
    ERROR,
    HOST,
    OK,
    PORT,
    RESULT,
    STATUS,
    build_submit,
    decode,
    encode,
)


class ProducerError(Exception):
    """Erro ao enviar uma tarefa ao Broker."""


class Producer:
    """Envia tarefas ao Broker da fila distribuída."""

    def __init__(
        self,
        producer_id: str | None = None,
        host: str = HOST,
        port: int = PORT,
        reconnect_interval: float = 3.0,
        max_retries: int = 5,
    ):
        self.producer_id = producer_id or f"producer-{uuid.uuid4().hex[:6]}"
        self.host = host
        self.port = port
        self.reconnect_interval = reconnect_interval
        self.max_retries = max_retries
        self.logger = logging.getLogger(f"Producer-{self.producer_id}")

    async def _connect(self) -> tuple[asyncio.StreamReader, asyncio.StreamWriter]:
        """Conecta ao Broker, repetindo em caso de falha."""
        for attempt in range(self.max_retries):
            try:
                return await asyncio.open_connection(self.host, self.port)
            except OSError as error:
                if attempt + 1 >= self.max_retries:
                    raise ProducerError(
                        f"Não foi possível conectar ao Broker após "
                        f"{self.max_retries} tentativa(s): {error}"
                    ) from error
                self.logger.warning(
                    "Falha ao conectar ao Broker (%s). Nova tentativa em %ss.",
                    error,
                    self.reconnect_interval,
                )
                await asyncio.sleep(self.reconnect_interval)
        raise ProducerError("Não foi possível conectar ao Broker.")

    async def _submit_on_connection(
        self,
        reader: asyncio.StreamReader,
        writer: asyncio.StreamWriter,
        payload: Any,
        task_id: str | None = None,
    ) -> str:
        """Envia uma tarefa pela conexão informada."""
        writer.write(encode(build_submit(payload, task_id)))
        await writer.drain()
        line = await reader.readline()
        if not line:
            raise ProducerError("Conexão fechada pelo Broker.")

        response = decode(line)
        response_type = response.get("type")
        if response_type == ERROR:
            raise ProducerError(response.get("error", "Erro retornado pelo Broker."))
        if response_type != OK or "task_id" not in response:
            raise ProducerError(f"Resposta inesperada do Broker: {response}")
        return response["task_id"]

    async def submit(self, payload: Any, task_id: str | None = None) -> str:
        """Envia uma tarefa e devolve o identificador confirmado pelo Broker."""
        reader, writer = await self._connect()
        try:
            return await self._submit_on_connection(reader, writer, payload, task_id)
        except (OSError, asyncio.IncompleteReadError) as error:
            raise ProducerError(f"Falha na comunicação com o Broker: {error}") from error
        finally:
            writer.close()
            await writer.wait_closed()

    async def submit_many(self, payloads: list[Any]) -> list[str]:
        """Envia várias tarefas pela mesma conexão."""
        reader, writer = await self._connect()
        task_ids = []
        try:
            for payload in payloads:
                task_ids.append(await self._submit_on_connection(reader, writer, payload))
            return task_ids
        except (OSError, asyncio.IncompleteReadError) as error:
            raise ProducerError(f"Falha na comunicação com o Broker: {error}") from error
        finally:
            writer.close()
            await writer.wait_closed()

    async def _send_request(self, message: dict) -> dict:
        """Envia uma consulta ao Broker e devolve a resposta."""
        reader, writer = await self._connect()
        try:
            writer.write(encode(message))
            await writer.drain()
            line = await reader.readline()
            if not line:
                raise ProducerError("Conexão fechada pelo Broker.")

            response = decode(line)
            if response.get("type") == ERROR:
                raise ProducerError(response.get("error", "Erro retornado pelo Broker."))
            return response
        except (OSError, asyncio.IncompleteReadError) as error:
            raise ProducerError(f"Falha na comunicação com o Broker: {error}") from error
        finally:
            writer.close()
            await writer.wait_closed()

    async def status(self) -> dict:
        """Consulta as quantidades de tarefas em cada estado no Broker."""
        response = await self._send_request({"type": STATUS})
        if response.get("type") != STATUS:
            raise ProducerError(f"Resposta inesperada do Broker: {response}")
        return response

    async def get_result(self, task_id: str) -> dict:
        """Consulta o estado e, quando disponível, o resultado de uma tarefa."""
        response = await self._send_request({"type": RESULT, "task_id": task_id})
        if response.get("type") != RESULT or response.get("task_id") != task_id:
            raise ProducerError(f"Resposta inesperada do Broker: {response}")
        return response

    async def wait_result(
        self, task_id: str, timeout: float = 30, poll: float = 0.5
    ) -> Any:
        """Aguarda o resultado final da tarefa ou lança ProducerError no timeout/falha."""
        if timeout < 0:
            raise ValueError("timeout não pode ser negativo")
        if poll <= 0:
            raise ValueError("poll deve ser maior que zero")

        deadline = time.monotonic() + timeout
        while True:
            response = await self.get_result(task_id)
            state = response["state"]
            if state == "done":
                return response["result"]
            if state == "failed":
                raise ProducerError(
                    f"Tarefa {task_id} falhou: {response.get('result')}"
                )

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ProducerError(
                    f"Tempo esgotado aguardando resultado da tarefa {task_id}."
                )
            await asyncio.sleep(min(poll, remaining))


def _build_payload(args: argparse.Namespace) -> Any:
    """Converte as opções da linha de comando no payload da tarefa."""
    if args.action is None:
        return args.payload if args.payload is not None else ""

    payload: dict[str, Any] = {"action": args.action, "duration": args.duration}
    if args.action in ("uppercase", "reverse"):
        payload["text"] = args.text if args.text is not None else ""
    else:
        payload["numbers"] = args.numbers if args.numbers is not None else []
    return payload


async def _run_cli(args: argparse.Namespace) -> None:
    producer = Producer(
        producer_id=args.producer_id,
        host=args.host,
        port=args.port,
    )
    payload = _build_payload(args)
    task_ids = []
    for index in range(args.count):
        task_id = await producer.submit(payload)
        task_ids.append(task_id)
        producer.logger.info("Tarefa enviada: %s", task_id)
        if index + 1 < args.count:
            await asyncio.sleep(args.interval)

    if args.wait:
        for task_id in task_ids:
            try:
                result = await producer.wait_result(task_id)
                print(f"Resultado da tarefa {task_id}: {result}")
            except ProducerError as error:
                print(f"Falha na tarefa {task_id}: {error}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Producer da Fila Distribuída de Tarefas")
    parser.add_argument("--id", dest="producer_id", default=None, help="Identificador do producer")
    parser.add_argument("--host", default=HOST, help=f"Endereço do Broker (padrão: {HOST})")
    parser.add_argument("--port", type=int, default=PORT, help=f"Porta do Broker (padrão: {PORT})")
    parser.add_argument("--payload", help="Payload simples em texto")
    parser.add_argument("--action", choices=("uppercase", "reverse", "sum"))
    parser.add_argument("--text", help="Texto para as ações uppercase e reverse")
    parser.add_argument("--numbers", type=int, nargs="+", help="Inteiros para a ação sum")
    parser.add_argument("--duration", type=float, default=0.5, help="Duração simulada da tarefa (s)")
    parser.add_argument("--count", type=int, default=1, help="Quantidade de tarefas a enviar")
    parser.add_argument("--interval", type=float, default=0, help="Intervalo entre envios (s)")
    parser.add_argument("--wait", action="store_true", help="Aguardar e imprimir os resultados")
    parser.add_argument("--debug", action="store_true", help="Habilitar logs de debug")
    args = parser.parse_args()
    if args.count < 1:
        parser.error("--count deve ser maior que zero")
    if args.interval < 0:
        parser.error("--interval não pode ser negativo")

    logging.basicConfig(
        level=logging.DEBUG if args.debug else logging.INFO,
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    try:
        asyncio.run(_run_cli(args))
    except KeyboardInterrupt:
        print("\n[Producer] Interrompido pelo usuário (Ctrl+C). Finalizando...")
    except ProducerError as error:
        logging.getLogger("Producer").error("%s", error)
        raise SystemExit(1) from error


if __name__ == "__main__":
    main()
