"""Demonstração local do Broker, dois Workers e cinco tarefas."""

import asyncio
from pathlib import Path
import subprocess
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from producer.producer import Producer  # noqa: E402


def _stop_processes(processes: list[subprocess.Popen]) -> None:
    """Encerra os processos filhos e força a parada se necessário."""
    for process in reversed(processes):
        if process.poll() is None:
            process.terminate()
    for process in reversed(processes):
        if process.poll() is None:
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()


async def _run_demo() -> None:
    processes = []
    try:
        processes.append(
            subprocess.Popen(
                [sys.executable, "-m", "broker.broker"],
                cwd=PROJECT_ROOT,
            )
        )
        for worker_id in ("demo-worker-1", "demo-worker-2"):
            processes.append(
                subprocess.Popen(
                    [
                        sys.executable,
                        str(PROJECT_ROOT / "worker" / "worker.py"),
                        "--id",
                        worker_id,
                    ],
                    cwd=PROJECT_ROOT,
                )
            )

        producer = Producer(
            producer_id="demo-producer",
            reconnect_interval=0.1,
            max_retries=60,
        )
        payloads = [
            {"action": "uppercase", "text": "fila distribuida", "duration": 0.1},
            {"action": "reverse", "text": "worker", "duration": 0.1},
            {"action": "sum", "numbers": [1, 2, 3], "duration": 0.1},
            {"action": "uppercase", "text": "sistemas distribuidos", "duration": 0.1},
            {"action": "reverse", "text": "broker", "duration": 0.1},
        ]
        task_ids = await producer.submit_many(payloads)

        results = []
        for task_id in task_ids:
            result = await producer.wait_result(task_id, timeout=30, poll=0.2)
            results.append((task_id, result))

        print("\nResumo da demonstração:")
        for task_id, result in results:
            print(f"- {task_id}: {result}")
        print(f"Total concluído: {len(results)}")
    finally:
        _stop_processes(processes)


def main() -> None:
    try:
        asyncio.run(_run_demo())
    except KeyboardInterrupt:
        print("\nDemonstração interrompida; processos encerrados.")


if __name__ == "__main__":
    main()
