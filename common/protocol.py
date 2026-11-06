import json
from typing import Any

HOST = "127.0.0.1"
PORT = 5000

SUBMIT = "SUBMIT"
REQUEST_TASK = "REQUEST_TASK"
TASK = "TASK"
NO_TASK = "NO_TASK"
ACK = "ACK"
FAIL = "FAIL"
OK = "OK"
ERROR = "ERROR"


def encode(msg: dict) -> bytes:
    return (json.dumps(msg) + "\n").encode()


def decode(line: bytes) -> dict:
    return json.loads(line.decode())


def build_submit(payload: Any, task_id: str | None = None) -> dict:
    """Monta uma mensagem SUBMIT com payload e identificador opcional."""
    task = {"payload": payload}
    if task_id is not None:
        task["id"] = task_id
    return {"type": SUBMIT, "task": task}