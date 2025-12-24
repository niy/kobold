import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlmodel import Session, SQLModel, create_engine

from kobosync.config import Settings, get_settings
from kobosync.database import get_session_dependency
from kobosync.job_queue import JobQueue
from kobosync.main import app

if TYPE_CHECKING:
    import threading
    from collections.abc import Callable, Generator

    from sqlalchemy.engine import Engine

pytest_plugins = [
    "tests.fixtures.mocks",
    "tests.fixtures.database",
    "tests.fixtures.files",
    "tests.fixtures.api",
    "tests.fixtures.worker",
]

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@pytest.fixture(scope="session")
def event_loop() -> Generator[asyncio.AbstractEventLoop]:
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@dataclass
class IntegrationContext:
    watch_dir: Path
    settings: Settings
    engine: Engine
    queue: JobQueue


@pytest.fixture
def integration_ctx(
    tmp_path: Path,
    start_worker: Callable[[Settings, Engine, JobQueue], threading.Thread],
) -> Generator[IntegrationContext]:
    watch_dir = tmp_path / "books"
    watch_dir.mkdir()

    test_settings = Settings(
        DATA_PATH=tmp_path,
        WATCH_DIRS=str(watch_dir),
        USER_TOKEN="test_token",
        CONVERT_EPUB=True,
    )

    test_engine = create_engine(
        test_settings.db_url, connect_args={"check_same_thread": False}
    )
    SQLModel.metadata.create_all(test_engine)

    test_queue = JobQueue(test_settings, test_engine)

    app.dependency_overrides[get_settings] = lambda: test_settings

    def get_test_session() -> Generator[Session]:
        with Session(test_engine) as session:
            yield session

    app.dependency_overrides[get_session_dependency] = get_test_session

    from unittest.mock import patch

    # Patch the global engine in database and main modules
    with (
        patch("kobosync.database.engine", test_engine),
        patch("kobosync.main.engine", test_engine),
    ):
        start_worker(test_settings, test_engine, test_queue)

        yield IntegrationContext(
            watch_dir=watch_dir,
            settings=test_settings,
            engine=test_engine,
            queue=test_queue,
        )

    app.dependency_overrides.clear()
