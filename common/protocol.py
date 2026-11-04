import json

HOST = "127.0.0.1"
PORT = 5000


def encode(msg: dict) -> bytes:
    return (json.dumps(msg) + "\n").encode()


def decode(line: bytes) -> dict:
    return json.loads(line.decode()) 