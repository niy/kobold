import asyncio
import contextlib
import sys
from collections.abc import AsyncGenerator
from dataclasses import dataclass
from pathlib import Path

import pytest
from sqlalchemy.engine import Engine
from sqlmodel import Session, SQLModel, create_engine

from kobold.config import Settings, get_settings
from kobold.database import get_session_dependency
from kobold.main import app
from kobold.task_queue import TaskQueue
from kobold.task_registry import create_tasks
from kobold.worker import worker

pytest_plugins = [
    "tests.fixtures.mocks",
    "tests.fixtures.database",
    "tests.fixtures.files",
    "tests.fixtures.api",
    "tests.fixtures.worker",
]

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture(scope="session")
def event_loop():
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@dataclass
class IntegrationContext:
    watch_dir: Path
    settings: Settings
    engine: Engine
    queue: TaskQueue


@pytest.fixture
async def integration_ctx(tmp_path: Path) -> AsyncGenerator[IntegrationContext]:
    watch_dir = tmp_path / "books"
    watch_dir.mkdir()

    test_settings = Settings(
        DATA_PATH=tmp_path,
        WATCH_DIRS=str(watch_dir),
        USER_TOKEN="test_token",
        CONVERT_EPUB=True,
        ORGANIZE_LIBRARY=True,
        ORGANIZE_TEMPLATE="{author}/{title}",
        WORKER_POLL_INTERVAL=0.01,
    )

    test_engine = create_engine(
        test_settings.db_url, connect_args={"check_same_thread": False}
    )
    SQLModel.metadata.create_all(test_engine)

    test_queue = TaskQueue(test_settings, test_engine)
    tasks = create_tasks(test_settings, test_engine, test_queue)

    app.dependency_overrides[get_settings] = lambda: test_settings

    def get_test_session():
        with Session(test_engine) as session:
            yield session

    app.dependency_overrides[get_session_dependency] = get_test_session

    from unittest.mock import patch

    with (
        patch("kobold.database.engine", test_engine),
        patch("kobold.main.engine", test_engine),
    ):
        worker_task = asyncio.create_task(
            worker(test_queue, tasks, test_settings.WORKER_POLL_INTERVAL)
        )

        yield IntegrationContext(
            watch_dir=watch_dir,
            settings=test_settings,
            engine=test_engine,
            queue=test_queue,
        )

        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task

    app.dependency_overrides.clear()
