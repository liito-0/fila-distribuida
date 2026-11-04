import asyncio
import logging
import uuid
from collections import deque

from common.protocol import HOST, PORT, decode, encode

logging.basicConfig(level=logging.INFO, format="%(asctime)s [BROKER] %(message)s")
log = logging.getLogger(__name__)

pending = deque()   # tarefas aguardando
in_progress = {}    # task_id -> {"task": ..., "worker": ...}
done = {}           # task_id -> resultado


def handle_message(msg: dict) -> dict:
    kind = msg.get("type")

    if kind == "SUBMIT":
        task = msg["task"]
        task.setdefault("id", str(uuid.uuid4()))
        pending.append(task)
        log.info("Tarefa recebida: %s", task["id"])
        return {"type": "OK", "task_id": task["id"]}

    if kind == "REQUEST_TASK":
        if not pending:
            return {"type": "NO_TASK"}
        task = pending.popleft()
        in_progress[task["id"]] = {"task": task, "worker": msg.get("worker_id")}
        log.info("Tarefa %s -> worker %s", task["id"], msg.get("worker_id"))
        return {"type": "TASK", "task": task}

    if kind == "ACK":
        in_progress.pop(msg["task_id"], None)
        done[msg["task_id"]] = msg.get("result")
        log.info("Tarefa %s concluída", msg["task_id"])
        return {"type": "OK"}

    if kind == "FAIL":
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
            except (ValueError, KeyError) as e:
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
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())