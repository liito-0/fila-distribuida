import asyncio
from common.protocol import HOST, PORT, decode, encode


async def enviar(msg):
    r, w = await asyncio.open_connection(HOST, PORT)
    w.write(encode(msg))
    await w.drain()
    resp = decode(await r.readline())
    w.close()
    return resp


async def main():
    print(await enviar({"type": "SUBMIT", "task": {"payload": "ola"}}))
    resp = await enviar({"type": "REQUEST_TASK", "worker_id": "w1"})
    print(resp)
    print(await enviar({"type": "ACK", "task_id": resp["task"]["id"], "result": "ok"}))

asyncio.run(main())