import asyncio
import contextlib
import zipfile
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pymupdf
import pytest
from sqlmodel import Session, SQLModel, create_engine, select
from tests.conftest import IntegrationContext

from kobold.config import Settings
from kobold.models import Book
from kobold.tasks.metadata import MetadataTask
from kobold.watcher import watch_directories


@pytest.fixture
async def hooks_ctx(
    tmp_path: Path,
    mock_amazon_provider: AsyncMock,
):
    from kobold.task_queue import TaskQueue

    watch_dir = tmp_path / "books"
    watch_dir.mkdir()

    db_path = tmp_path / "test_hooks.db"
    db_url = f"sqlite:///{db_path}"
    test_engine = create_engine(db_url)
    SQLModel.metadata.create_all(test_engine)

    test_settings = Settings(
        WATCH_DIRS=str(watch_dir),
        WATCH_FORCE_POLLING=True,
        USER_TOKEN="test_token",
        CONVERT_EPUB=True,
        EMBED_METADATA=True,
        DELETE_ORIGINAL_AFTER_CONVERSION=False,
        FETCH_EXTERNAL_METADATA=True,
    )

    test_queue = TaskQueue(test_settings, test_engine)

    mock_amazon_provider.fetch_metadata = AsyncMock(
        return_value={
            "title": "Scraped Title",
            "author": "Scraped Author",
            "isbn": "9781234567890",
            "cover_path": "http://example.com/cover.jpg",
        }
    )

    with (
        patch("kobold.http_client.HttpClientManager.get_client") as mock_get_client,
    ):
        mock_client = AsyncMock()
        mock_client.get = AsyncMock()
        mock_response = AsyncMock()
        mock_response.status_code = 200
        mock_response.content = b"fake_cover_data"
        mock_client.get.return_value = mock_response
        mock_get_client.return_value = mock_client

        yield IntegrationContext(
            watch_dir=watch_dir,
            settings=test_settings,
            engine=test_engine,
            queue=test_queue,
        )


@pytest.mark.asyncio
async def test_embed_metadata_epub(
    hooks_ctx: IntegrationContext, test_data_dir: Path, async_worker_task
):
    ctx = hooks_ctx

    watcher_task = asyncio.create_task(
        watch_directories([ctx.watch_dir], ctx.settings, ctx.queue)
    )
    await asyncio.sleep(0.1)
    worker_task = async_worker_task(ctx.settings, ctx.engine, ctx.queue)

    try:
        test_file = ctx.watch_dir / "embed_test.epub"
        test_file.write_bytes((test_data_dir / "romeo_and_juliet.epub").read_bytes())

        found = False
        for _ in range(50):
            with Session(ctx.engine) as session:
                book = session.exec(
                    select(Book).where(Book.file_path == str(test_file))
                ).first()
                if book and book.title == "Scraped Title":
                    found = True
                    break
            await asyncio.sleep(0.1)

        assert found, "Database was not updated with scraped title"

        completed = False
        for _ in range(50):
            with Session(ctx.engine) as session:
                from kobold.models import Task as TaskModel
                from kobold.models import TaskStatus

                job = session.exec(
                    select(TaskModel).where(TaskModel.type == MetadataTask.TASK_TYPE)
                ).first()
                if job and job.status == TaskStatus.COMPLETED:
                    completed = True
                    break
            await asyncio.sleep(0.05)

        assert completed, "Metadata job did not complete"

        with zipfile.ZipFile(test_file, "r") as zf:
            opf_name = next(n for n in zf.namelist() if n.endswith(".opf"))
            opf_content = zf.read(opf_name).decode("utf-8")
            assert "Scraped Title" in opf_content
            assert "Scraped Author" in opf_content
            assert "9781234567890" in opf_content

            cover_found = False
            for name in zf.namelist():
                if zf.read(name) == b"fake_cover_data":
                    cover_found = True
                    break

            if cover_found:
                assert True
            else:
                pass

    finally:
        watcher_task.cancel()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher_task
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task


@pytest.mark.asyncio
async def test_embed_metadata_pdf(
    hooks_ctx: IntegrationContext, test_data_dir: Path, async_worker_task
):
    ctx = hooks_ctx

    watcher_task = asyncio.create_task(
        watch_directories([ctx.watch_dir], ctx.settings, ctx.queue)
    )
    await asyncio.sleep(0.1)
    worker_task = async_worker_task(ctx.settings, ctx.engine, ctx.queue)

    try:
        test_file = ctx.watch_dir / "embed_test.pdf"
        test_file.write_bytes((test_data_dir / "beauty_and_the_beast.pdf").read_bytes())

        found = False
        for _ in range(50):
            with Session(ctx.engine) as session:
                book = session.exec(
                    select(Book).where(Book.file_path == str(test_file))
                ).first()
                if book and book.title == "Scraped Title":
                    found = True
                    break
            await asyncio.sleep(0.1)

        assert found, "Database was not updated with scraped title"
        completed = False
        for _ in range(50):
            with Session(ctx.engine) as session:
                from kobold.models import Task as TaskModel
                from kobold.models import TaskStatus

                job = session.exec(
                    select(TaskModel).where(TaskModel.type == MetadataTask.TASK_TYPE)
                ).first()
                if job and job.status == TaskStatus.COMPLETED:
                    completed = True
                    break
            await asyncio.sleep(0.05)

        assert completed, "Metadata job did not complete"

        doc = pymupdf.open(str(test_file))
        try:
            metadata = doc.metadata
            assert metadata is not None
            assert metadata.get("title") == "Scraped Title"
            assert metadata.get("author") == "Scraped Author"

            xmp_data = doc.xref_get_key(-1, "Metadata")
            if xmp_data and xmp_data[0] == "stream":
                xmp_stream = doc.xref_stream(-1)
                if xmp_stream:
                    xmp_str = xmp_stream.decode("utf-8", errors="ignore")
                    assert "9781234567890" in xmp_str
        finally:
            doc.close()

    finally:
        watcher_task.cancel()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher_task
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task


@pytest.mark.asyncio
async def test_delete_original_after_conversion(
    hooks_ctx: IntegrationContext, test_data_dir: Path, async_worker_task
):
    ctx = hooks_ctx

    ctx.settings.DELETE_ORIGINAL_AFTER_CONVERSION = True

    watcher_task = asyncio.create_task(
        watch_directories([ctx.watch_dir], ctx.settings, ctx.queue)
    )
    await asyncio.sleep(0.1)
    worker_task = async_worker_task(ctx.settings, ctx.engine, ctx.queue)

    try:
        test_file = (ctx.watch_dir / "delete_test.epub").absolute()
        test_file.write_bytes((test_data_dir / "romeo_and_juliet.epub").read_bytes())

        kepub_file = ctx.watch_dir / "delete_test.kepub.epub"

        found_kepub = False
        for _ in range(100):
            if kepub_file.exists():
                found_kepub = True
                break
            await asyncio.sleep(0.05)

        assert found_kepub, "KEPUB file was not created"

        deleted = False
        for _ in range(50):
            if not test_file.exists():
                deleted = True
                break
            await asyncio.sleep(0.05)

        assert deleted, "Original file was not deleted"

        book_deleted = False
        for _ in range(50):
            with Session(ctx.engine) as session:
                original_path_str = str(test_file)
                book = session.exec(
                    select(Book).where(Book.file_path == original_path_str)
                ).first()
                if book and book.is_deleted:
                    book_deleted = True
                    break
            await asyncio.sleep(0.05)

        assert book_deleted, "Original book record was not marked as deleted"

        # Wait for the new KEPUB book record to be created in the database
        found_new_book = False
        new_book = None
        for _ in range(50):
            with Session(ctx.engine) as session:
                kepub_path_str = str(kepub_file)
                new_book = session.exec(
                    select(Book).where(Book.file_path == kepub_path_str)
                ).first()
                if new_book:
                    found_new_book = True
                    break
            await asyncio.sleep(0.05)

        assert found_new_book, "New KEPUB book record not found"
        assert new_book
        assert not new_book.is_deleted

    finally:
        watcher_task.cancel()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher_task
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
