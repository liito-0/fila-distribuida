import asyncio
from contextlib import suppress
import logging
import time
import uuid
from collections import deque

<<<<<<< HEAD
from common.protocol import HOST, PORT, decode, encode
from worker import worker
=======
from common.protocol import HOST, PORT, RESULT, STATUS, decode, encode
>>>>>>> origin/main

TASK_TIMEOUT_SECONDS = 30
TIMEOUT_CHECK_INTERVAL_SECONDS = 5
MAX_ATTEMPTS = 3

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BROKER] %(message)s")
log = logging.getLogger(__name__)

pending = deque()   # tarefas aguardando
in_progress = {}    # task_id -> {"task": ..., "worker": ..., "started_at": ...}
done = {}           # task_id -> resultado
failed = {}         # task_id -> motivo da falha
attempts = {}       # task_id -> número de entregas


def _release_tasks(task_ids, reason: str, *, to_front: bool) -> None:
    tasks_to_requeue = []
    for task_id in task_ids:
        info = in_progress.pop(task_id, None)
        if info is None:
            continue

        # Ao atingir o limite, a tarefa fica registrada em failed em vez de voltar à fila.
        if attempts.get(task_id, 0) >= MAX_ATTEMPTS:
            failed[task_id] = reason
            log.warning(
                "Tarefa %s atingiu o limite de tentativas e foi marcada como falha: %s",
                task_id,
                reason,
            )
        else:
            tasks_to_requeue.append(info["task"])

    if to_front:
        pending.extendleft(reversed(tasks_to_requeue))
    else:
        pending.extend(tasks_to_requeue)


def _requeue_expired_tasks(now: float | None = None) -> None:
    if now is None:
        now = time.monotonic()

    expired_tasks = []
    for task_id, info in list(in_progress.items()):
        if now - info["started_at"] > TASK_TIMEOUT_SECONDS:
            if attempts.get(task_id, 0) >= MAX_ATTEMPTS:
                _release_tasks(
                    [task_id],
                    "timeout",
                    to_front=True,
                )
                log.warning("Tarefa %s expirou sem ACK/FAIL", task_id)
            else:
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
        attempts[task["id"]] = attempts.get(task["id"], 0) + 1
        log.info("Tarefa %s -> worker %s", task["id"], msg.get("worker_id"))
        return {"type": "TASK", "task": task}

    if kind == "ACK":
        task_id = msg["task_id"]
<<<<<<< HEAD
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
=======
        if task_id not in in_progress:
            log.warning("ACK ignorado para tarefa %s fora de in_progress", task_id)
            return {"type": "OK"}
        in_progress.pop(task_id)
        done[task_id] = msg.get("result")
        log.info("Tarefa %s concluída", task_id)
        return {"type": "OK"}

    if kind == "FAIL":
        task_id = msg["task_id"]
        info = in_progress.get(task_id)
        if info is None:
            log.warning("FAIL ignorado para tarefa %s fora de in_progress", task_id)
            return {"type": "OK"}

        in_progress.pop(task_id)
        error = msg.get("error")
        if attempts.get(task_id, 0) >= MAX_ATTEMPTS:
            failed[task_id] = error
            log.warning(
                "Tarefa %s atingiu o limite de tentativas e foi marcada como falha: %s",
                task_id,
                error,
            )
        else:
            pending.append(info["task"])
        log.warning("Tarefa %s falhou: %s", task_id, error)
>>>>>>> origin/main
        return {"type": "OK"}

    if kind == STATUS:
        return {
            "type": "STATUS",
            "pending": len(pending),
            "in_progress": len(in_progress),
            "done": len(done),
            "failed": len(failed),
        }

    if kind == RESULT:
        task_id = msg["task_id"]
        if task_id in in_progress:
            state = "in_progress"
        elif any(task.get("id") == task_id for task in pending):
            state = "pending"
        elif task_id in done:
            state = "done"
        elif task_id in failed:
            state = "failed"
        else:
            state = "unknown"

        response = {"type": RESULT, "task_id": task_id, "state": state}
        if state == "done":
            response["result"] = done[task_id]
        elif state == "failed":
            response["result"] = failed[task_id]
        return response

    return {"type": "ERROR", "error": f"tipo desconhecido: {kind}"}


async def handle_client(reader, writer):
    peer = writer.get_extra_info("peername")
    # Mantém as tarefas desta conexão para devolvê-las se ela cair sem ACK/FAIL.
    delivered_tasks = {}
    log.info("Conectado: %s", peer)
    try:
        while line := await reader.readline():
            try:
                msg = decode(line)
                reply = handle_message(msg)
                if msg.get("type") == "REQUEST_TASK" and reply.get("type") == "TASK":
                    task_id = reply["task"]["id"]
                    delivered_tasks[task_id] = None
                elif msg.get("type") in ("ACK", "FAIL"):
                    delivered_tasks.pop(msg.get("task_id"), None)
            except (ValueError, KeyError, TypeError, AttributeError) as e:
                reply = {"type": "ERROR", "error": str(e)}
            writer.write(encode(reply))
            await writer.drain()
    finally:
        _release_tasks(
            delivered_tasks,
            "worker desconectado",
            to_front=True,
        )
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
