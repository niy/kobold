import asyncio
import threading
from collections.abc import Callable, Generator

import pytest
from sqlalchemy.engine import Engine

from kobosync.config import Settings
from kobosync.job_queue import JobQueue


@pytest.fixture
def start_worker() -> Generator[
    Callable[[Settings, Engine, JobQueue], threading.Thread]
]:
    from kobosync.worker import stop_event, worker

    threads = []

    def start_worker(
        settings: Settings, engine: Engine, queue: JobQueue
    ) -> threading.Thread:
        stop_event.clear()

        def run_worker_wrapper() -> None:
            try:
                import asyncio

                asyncio.run(worker(settings, engine, queue))
            except Exception as e:
                print(f"Worker thread error: {e}")

        thread = threading.Thread(target=run_worker_wrapper, daemon=True)
        thread.start()
        threads.append(thread)
        return thread

    yield start_worker

    stop_event.set()
    for t in threads:
        t.join(timeout=2.0)


@pytest.fixture
def async_worker_task() -> Generator[
    Callable[[Settings, Engine, JobQueue], asyncio.Task[None]]
]:
    from kobosync.worker import stop_event, worker

    tasks: list[asyncio.Task[None]] = []

    def create_task(
        settings: Settings, engine: Engine, queue: JobQueue
    ) -> asyncio.Task[None]:
        stop_event.clear()
        task = asyncio.create_task(worker(settings, engine, queue))
        tasks.append(task)
        return task

    yield create_task

    stop_event.set()
    for task in tasks:
        task.cancel()
