"""Pipeline integration tests.

Tests the complete processing pipeline: INGEST → METADATA → CONVERT → ORGANIZE → API.
For true E2E tests including the watcher, see tests/smoke/test_docker.py.
"""

import asyncio
from pathlib import Path
from typing import Any

import pytest
from httpx import ASGITransport, AsyncClient
from sqlmodel import Session, col, select
from tests.conftest import IntegrationContext

from kobold.main import app
from kobold.models import Book, TaskStatus
from kobold.models import Task as TaskModel


async def wait_for_tasks(engine: Any, timeout_sec: float = 5.0) -> None:
    try:
        async with asyncio.timeout(timeout_sec):
            while True:
                with Session(engine) as session:
                    pending = session.exec(
                        select(TaskModel).where(
                            col(TaskModel.status).in_(
                                [TaskStatus.PENDING, TaskStatus.PROCESSING]
                            )
                        )
                    ).all()
                    if not pending:
                        return
                await asyncio.sleep(0.05)
    except TimeoutError:
        pass


class TestFullPipeline:
    @pytest.mark.asyncio
    async def test_epub_pipeline(
        self, integration_ctx: IntegrationContext, test_data_dir: Path
    ):
        ctx = integration_ctx

        original_file = ctx.watch_dir / "romeo.epub"
        original_file.write_bytes(
            (test_data_dir / "romeo_and_juliet.epub").read_bytes()
        )

        from kobold.scanner import ScannerService

        scanner = ScannerService(settings=ctx.settings, queue=ctx.queue)
        await scanner.scan_directories()
        await wait_for_tasks(ctx.engine)

        with Session(ctx.engine) as session:
            book = session.exec(
                select(Book).where(col(Book.title).contains("Romeo"))
            ).first()

            assert book is not None, "INGEST: Book not found"
            book_id = str(book.id)
            assert "romeo" in book.title.lower(), "METADATA: Title not extracted"
            assert book.kepub_path is not None, "CONVERT: kepub_path not set"
            assert Path(book.kepub_path).exists(), "CONVERT: kepub file missing"

            new_path = Path(book.file_path)
            assert new_path.exists(), "ORGANIZE: File missing"
            assert new_path != original_file, "ORGANIZE: File not moved"

        # Use AsyncClient with ASGITransport to avoid lifespan conflicts
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test") as client:
            resp = await client.get(
                "/api/kobo/test_token/v1/library/sync",
                headers={"X-Kobo-SyncToken": "0"},
            )
            assert resp.status_code == 200
            assert any(
                item["NewEntitlement"]["EntitlementId"] == book_id
                for item in resp.json()
            ), "API: Book not in sync"

            dl_resp = await client.get(f"/download/{book_id}")
            assert dl_resp.status_code == 200
            assert len(dl_resp.content) > 0, "API: Empty download"

    @pytest.mark.asyncio
    async def test_pdf_pipeline(
        self, integration_ctx: IntegrationContext, test_data_dir: Path
    ):
        ctx = integration_ctx

        original_file = ctx.watch_dir / "beauty.pdf"
        original_file.write_bytes(
            (test_data_dir / "beauty_and_the_beast.pdf").read_bytes()
        )

        from kobold.scanner import ScannerService

        scanner = ScannerService(settings=ctx.settings, queue=ctx.queue)
        await scanner.scan_directories()
        await wait_for_tasks(ctx.engine)

        with Session(ctx.engine) as session:
            book = session.exec(select(Book).where(Book.file_format == "pdf")).first()

            assert book is not None, "INGEST: PDF not found"
            assert book.title and len(book.title) > 0, "METADATA: Title missing"
            assert Path(book.file_path).exists(), "ORGANIZE: File missing"

    @pytest.mark.asyncio
    async def test_cbz_pipeline(self, integration_ctx: IntegrationContext):
        import zipfile

        ctx = integration_ctx

        original_file = ctx.watch_dir / "comic.cbz"
        with zipfile.ZipFile(original_file, "w") as zf:
            zf.writestr("page1.jpg", b"fake_image_content")

        from kobold.scanner import ScannerService

        scanner = ScannerService(settings=ctx.settings, queue=ctx.queue)
        await scanner.scan_directories()
        await wait_for_tasks(ctx.engine)

        with Session(ctx.engine) as session:
            book = session.exec(select(Book).where(Book.file_format == "cbz")).first()

            assert book is not None, "INGEST: CBZ not found"
            assert "comic" in book.title.lower(), "METADATA: Title missing"
            assert Path(book.file_path).exists(), "ORGANIZE: File missing"
