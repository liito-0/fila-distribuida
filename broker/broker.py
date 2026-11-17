import asyncio
from contextlib import suppress
import logging
import time
import uuid
from collections import deque

from common.protocol import HOST, PORT, decode, encode
from worker import worker

TASK_TIMEOUT_SECONDS = 30
TIMEOUT_CHECK_INTERVAL_SECONDS = 5

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BROKER] %(message)s")
log = logging.getLogger(__name__)

pending = deque()   # tarefas aguardando
in_progress = {}    # task_id -> {"task": ..., "worker": ..., "started_at": ...}
done = {}           # task_id -> resultado


def _requeue_expired_tasks(now: float | None = None) -> None:
    if now is None:
        now = time.monotonic()

    expired_tasks = []
    for task_id, info in list(in_progress.items()):
        if now - info["started_at"] > TASK_TIMEOUT_SECONDS:
            in_progress.pop(task_id)
            expired_tasks.append((task_id, info["task"]))
            log.warning(
                "Tarefa %s expirou sem ACK/FAIL; retornando ao início da fila",
                task_id,
            )
    pending.extendleft(task for _, task in reversed(expired_tasks))


async def _monitor_timeouts() -> None:
    while True:
        await asyncio.sleep(TIMEOUT_CHECK_INTERVAL_SECONDS)
        _requeue_expired_tasks()


def handle_message(msg: dict) -> dict:
    kind = msg.get("type")

    if kind == "SUBMIT":
        task = msg.get("task")
        if not isinstance(task, dict):
            return {"type": "ERROR", "error": "task inválida"}
        task.setdefault("id", str(uuid.uuid4()))
        task_id = task["id"]
        if (task_id in in_progress or task_id in done or any(t.get("id") == task_id for t in pending)): #verificando se o task_id ja esta em algum lugar 
            return {"type": "ERROR", "error":f"task_id duplicado: {task_id}"}
        pending.append(task)
        log.info("Tarefa recebida: %s", task["id"])
        return {"type": "OK", "task_id": task["id"]}

    if kind == "REQUEST_TASK":
        if not pending:
            return {"type": "NO_TASK"}
        task = pending.popleft()
        in_progress[task["id"]] = {
            "task": task,
            "worker": msg.get("worker_id"),
            "started_at": time.monotonic(),
        }
        log.info("Tarefa %s -> worker %s", task["id"], msg.get("worker_id"))
        return {"type": "TASK", "task": task}

    if kind == "ACK":
        task_id = msg["task_id"]
        info = in_progress.get(task_id)
        worker_id = msg.get("worker_id")
        if info is None or (worker_id is not None and info.get("worker") != worker_id):
            return {"type": "ERROR", "error": "tarefa inválida ou atribuída a outro worker"}

        in_progress.pop(msg["task_id"], None)
        done[msg["task_id"]] = msg.get("result")
        log.info("Tarefa %s concluída", msg["task_id"])
        return {"type": "OK"}

    if kind == "FAIL":

        task_id = msg["task_id"]
        info = in_progress.get(task_id)
        worker_id = msg.get("worker_id")
        if info is None or (worker_id is not None and info.get("worker") != worker_id):
            return {"type": "ERROR", "error": "tarefa inválida ou atribuída a outro worker"}
        info = in_progress.pop(msg["task_id"], None)
        if info:
            pending.append(info["task"])  # devolve à fila
        log.warning("Tarefa %s falhou: %s", msg["task_id"], msg.get("error"))
        return {"type": "OK"}

    return {"type": "ERROR", "error": f"tipo desconhecido: {kind}"}


async def handle_client(reader, writer):
    peer = writer.get_extra_info("peername")
    log.info("Conectado: %s", peer)
    try:
        while line := await reader.readline():
            try:
                reply = handle_message(decode(line))
            except (ValueError, KeyError, TypeError, AttributeError) as e:
                reply = {"type": "ERROR", "error": str(e)}
            writer.write(encode(reply))
            await writer.drain()
    finally:
        writer.close()
        log.info("Desconectado: %s", peer)


async def main():
    server = await asyncio.start_server(handle_client, HOST, PORT)
    log.info("Escutando em %s:%s", HOST, PORT)
    async with server:
        timeout_monitor = asyncio.create_task(_monitor_timeouts())
        try:
            await server.serve_forever()
        finally:
            timeout_monitor.cancel()
            with suppress(asyncio.CancelledError):
                await timeout_monitor


if __name__ == "__main__":
    asyncio.run(main())
