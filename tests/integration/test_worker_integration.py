import asyncio
import contextlib
from pathlib import Path
from typing import TYPE_CHECKING

import pytest
from sqlmodel import Session, col, select

from kobosync.models import Book, Job
from kobosync.watcher import watch_directories
from kobosync.worker import worker

if TYPE_CHECKING:
    from tests.conftest import IntegrationContext


@pytest.mark.asyncio
async def test_worker_processing_flow(integration_ctx: IntegrationContext):
    ctx = integration_ctx

    watcher_task = asyncio.create_task(
        watch_directories([ctx.watch_dir], ctx.settings, ctx.queue)
    )

    worker_task = asyncio.create_task(worker(ctx.settings, ctx.engine, ctx.queue))

    try:
        await asyncio.sleep(0.1)

        source_epub = Path("tests/data/romeo_and_juliet.epub")
        if not source_epub.exists():
            pytest.fail("Real EPUB file not found (tests/data/romeo_and_juliet.epub)")

        book_path = ctx.watch_dir / "romeo.epub"
        book_path.write_bytes(source_epub.read_bytes())

        found_book = False
        target_title = "Romeo and Juliet"

        for _ in range(100):
            with Session(ctx.engine) as session:
                book = session.exec(
                    select(Book).where(col(Book.title).contains(target_title))
                ).first()
                if book:
                    found_book = True
                    break
            await asyncio.sleep(0.1)

        assert found_book, f"Book '{target_title}' not found in DB"

        with Session(ctx.engine) as session:
            jobs = session.exec(select(Job)).all()
            assert len(jobs) > 0

            book = session.exec(
                select(Book).where(col(Book.title).contains(target_title))
            ).one()

            assert "Shakespeare" in (book.author or "")
            assert book.file_path == str(book_path)
            assert not book.is_deleted

    finally:
        watcher_task.cancel()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher_task
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task


@pytest.mark.asyncio
async def test_worker_pdf_processing(integration_ctx: IntegrationContext):
    ctx = integration_ctx

    watcher_task = asyncio.create_task(
        watch_directories([ctx.watch_dir], ctx.settings, ctx.queue)
    )

    worker_task = asyncio.create_task(worker(ctx.settings, ctx.engine, ctx.queue))

    try:
        await asyncio.sleep(0.1)

        source_pdf = Path("tests/data/beauty_and_the_beast.pdf")
        if not source_pdf.exists():
            pytest.fail("Real PDF file not found (tests/data/beauty_and_the_beast.pdf)")

        book_path = ctx.watch_dir / "beauty.pdf"
        book_path.write_bytes(source_pdf.read_bytes())

        found_book = False
        target_title = "Beauty"

        for _ in range(100):
            with Session(ctx.engine) as session:
                book = session.exec(
                    select(Book).where(col(Book.title).contains(target_title))
                ).first()
                if book and book.file_format == "pdf":
                    found_book = True
                    break
            await asyncio.sleep(0.1)

        assert found_book, "PDF Book not processed or title mismatch"

        with Session(ctx.engine) as session:
            book = session.exec(
                select(Book).where(col(Book.title).contains(target_title))
            ).one()
            assert book.file_format == "pdf"
            assert book.title is not None
            assert book.file_path == str(book_path)

    finally:
        watcher_task.cancel()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await watcher_task
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task
