"""Integration tests for the async file watcher.

Tests the watchfiles-based watcher to ensure it correctly detects
file events and enqueues tasks appropriately.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator, Generator
from pathlib import Path
from typing import Any

import pytest
from sqlalchemy import create_engine
from sqlmodel import Session, SQLModel, select

from kobold.config import Settings
from kobold.models import Task
from kobold.task_queue import TaskQueue
from kobold.tasks.ingest import IngestTask
from kobold.watcher import watch_directories


@pytest.fixture
def watch_dir(tmp_path: Path) -> Path:
    d = tmp_path / "books"
    d.mkdir()
    return d


@pytest.fixture
def test_db_engine(tmp_path: Path) -> Generator[Any]:
    db_path = tmp_path / "test.db"
    engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(engine)
    yield engine


@pytest.fixture
def test_settings() -> Settings:
    return Settings(WATCH_FORCE_POLLING=False, USER_TOKEN="test_token")


@pytest.fixture
def test_queue(test_settings: Settings, test_db_engine: Any) -> TaskQueue:
    return TaskQueue(test_settings, test_db_engine)


@pytest.fixture
async def watcher_lifecycle(
    watch_dir: Path,
    test_settings: Settings,
    test_queue: TaskQueue,
) -> AsyncGenerator[asyncio.Task[None]]:
    """Start a watcher task and clean it up after test."""
    watcher_task = asyncio.create_task(
        watch_directories([watch_dir], test_settings, test_queue)
    )
    # Give watcher time to start
    await asyncio.sleep(0.1)

    yield watcher_task

    watcher_task.cancel()
    with contextlib.suppress(asyncio.CancelledError, TimeoutError):
        await asyncio.wait_for(watcher_task, timeout=2.0)


async def wait_for_task(
    engine: Any, event_type: str, path: str, max_wait: float = 3.0
) -> Task | None:
    """Poll database for a task matching the event and path."""
    start_time = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start_time < max_wait:
        with Session(engine) as session:
            tasks = session.exec(select(Task)).all()
            for task in tasks:
                if (
                    task.payload.get("event") == event_type
                    and task.payload.get("path") == path
                ):
                    return task
        await asyncio.sleep(0.05)
    return None


async def clear_tasks(engine: Any) -> None:
    """Clear all tasks from database."""
    with Session(engine) as session:
        for t in session.exec(select(Task)).all():
            session.delete(t)
        session.commit()


@pytest.mark.integration
async def test_watcher_detects_file_creation(
    watch_dir: Path,
    test_db_engine: Any,
    watcher_lifecycle: asyncio.Task[None],
) -> None:
    """Test that watcher detects new file creation."""
    book_path = watch_dir / "test.epub"
    book_path.touch()

    path_str = str(book_path.absolute())
    task = await wait_for_task(test_db_engine, "ADD", path_str)

    assert task is not None
    assert task.type == IngestTask.TASK_TYPE
    assert task.payload["event"] == "ADD"
    assert task.payload["path"] == path_str


@pytest.mark.integration
async def test_watcher_detects_file_deletion(
    watch_dir: Path,
    test_db_engine: Any,
    watcher_lifecycle: asyncio.Task[None],
) -> None:
    """Test that watcher detects file deletion."""
    book_path = watch_dir / "to_delete.epub"
    book_path.touch()

    path_str = str(book_path.absolute())
    await wait_for_task(test_db_engine, "ADD", path_str)
    await clear_tasks(test_db_engine)

    book_path.unlink()

    delete_task = await wait_for_task(test_db_engine, "DELETE", path_str)
    assert delete_task is not None
    assert delete_task.type == IngestTask.TASK_TYPE
    assert delete_task.payload["event"] == "DELETE"


@pytest.mark.integration
async def test_watcher_detects_file_rename(
    watch_dir: Path,
    test_db_engine: Any,
    watcher_lifecycle: asyncio.Task[None],
) -> None:
    """Test that watcher detects file rename."""
    original = watch_dir / "original.epub"
    renamed = watch_dir / "renamed.epub"

    original.touch()
    await wait_for_task(test_db_engine, "ADD", str(original.absolute()))
    await clear_tasks(test_db_engine)

    original.rename(renamed)

    renamed_str = str(renamed.absolute())
    add_task = await wait_for_task(test_db_engine, "ADD", renamed_str)
    assert add_task is not None
    assert add_task.payload["path"] == renamed_str


@pytest.mark.integration
async def test_watcher_ignores_unsupported_files(
    watch_dir: Path,
    test_db_engine: Any,
    watcher_lifecycle: asyncio.Task[None],
) -> None:
    """Test that watcher ignores non-ebook files."""
    txt_path = watch_dir / "notes.txt"
    txt_path.touch()

    await asyncio.sleep(0.3)

    with Session(test_db_engine) as session:
        tasks = session.exec(select(Task)).all()
        assert len(tasks) == 0


@pytest.mark.integration
async def test_watcher_polling_mode(
    watch_dir: Path,
    test_db_engine: Any,
    test_queue: TaskQueue,
) -> None:
    """Test that polling mode works for network shares."""
    polling_settings = Settings(WATCH_FORCE_POLLING=True, USER_TOKEN="test_token")

    watcher_task = asyncio.create_task(
        watch_directories([watch_dir], polling_settings, test_queue)
    )
    await asyncio.sleep(0.2)

    try:
        book_path = watch_dir / "polling_test.epub"
        book_path.touch()

        path_str = str(book_path.absolute())
        task = await wait_for_task(test_db_engine, "ADD", path_str, max_wait=5.0)

        assert task is not None
        assert task.payload["path"] == path_str
    finally:
        watcher_task.cancel()
        with contextlib.suppress(asyncio.CancelledError, TimeoutError):
            await asyncio.wait_for(watcher_task, timeout=2.0)
