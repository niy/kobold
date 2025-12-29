"""Worker fixtures for testing."""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Callable

import pytest
from sqlalchemy.engine import Engine

from kobold.config import Settings
from kobold.task_queue import TaskQueue
from kobold.task_registry import create_tasks
from kobold.worker import worker


@pytest.fixture
async def async_worker_task() -> AsyncGenerator[
    Callable[[Settings, Engine, TaskQueue], asyncio.Task[None]]
]:
    tasks_list: list[asyncio.Task[None]] = []

    def create_task(
        settings: Settings, engine: Engine, queue: TaskQueue
    ) -> asyncio.Task[None]:
        tasks = create_tasks(settings, engine, queue)
        task = asyncio.create_task(worker(queue, tasks, settings.WORKER_POLL_INTERVAL))
        tasks_list.append(task)
        return task

    yield create_task

    for task in tasks_list:
        task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await task
