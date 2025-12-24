import asyncio
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from sqlmodel import Session, col, select
from tests.conftest import IntegrationContext

from kobosync.models import Book, Job, JobStatus


@pytest.fixture
def real_files_setup(integration_ctx, client):
    ctx = integration_ctx
    ctx.client = client
    yield ctx


async def wait_for_jobs(engine, timeout_sec=5.0):
    try:
        async with asyncio.timeout(timeout_sec):
            while True:
                with Session(engine) as session:
                    pending = session.exec(
                        select(Job).where(
                            col(Job.status).in_(
                                [JobStatus.PENDING, JobStatus.PROCESSING]
                            )
                        )
                    ).all()
                    if not pending:
                        return
                await asyncio.sleep(0.05)
    except TimeoutError:
        pass


@pytest.mark.asyncio
async def test_full_pipeline_epub(
    integration_ctx: IntegrationContext, test_data_dir: Path, client: TestClient
):
    ctx = integration_ctx

    test_file = ctx.watch_dir / "romeo.epub"
    test_file.write_bytes((test_data_dir / "romeo_and_juliet.epub").read_bytes())

    from kobosync.scanner import ScannerService

    scanner = ScannerService(settings=ctx.settings, queue=ctx.queue)
    await scanner.scan_directories()

    await wait_for_jobs(ctx.engine)

    with Session(ctx.engine) as session:
        book = session.exec(
            select(Book).where(Book.file_path == str(test_file))
        ).first()
        assert book is not None, "Book not found in DB"
        assert "romeo" in book.title.lower()
        book_id = str(book.id)

    token = "test_token"
    resp = client.get(
        f"/api/kobo/{token}/v1/library/sync", headers={"X-Kobo-SyncToken": "0"}
    )
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) >= 1

    found = False
    for item in data:
        if item["NewEntitlement"]["EntitlementId"] == book_id:
            found = True
            break
    assert found, "Synced book not found in API response"

    dl_resp = client.get(f"/download/{book_id}")
    assert dl_resp.status_code == 200

    if ctx.settings.CONVERT_EPUB:
        assert dl_resp.headers["content-type"] == "application/epub+zip"
        assert dl_resp.content == b"mock kepub content"
    else:
        assert dl_resp.headers["content-type"] == "application/epub+zip"
        assert (
            len(dl_resp.content)
            == (test_data_dir / "romeo_and_juliet.epub").stat().st_size
        )


@pytest.mark.asyncio
async def test_comics_ingestion(
    integration_ctx: IntegrationContext,
):
    ctx = integration_ctx

    import zipfile

    cbz_path = ctx.watch_dir / "comic.cbz"
    with zipfile.ZipFile(cbz_path, "w") as zf:
        zf.writestr("page1.jpg", b"fake_image_content")

    from kobosync.scanner import ScannerService

    scanner = ScannerService(settings=ctx.settings, queue=ctx.queue)
    await scanner.scan_directories()

    await wait_for_jobs(ctx.engine)

    with Session(ctx.engine) as session:
        book = session.exec(select(Book).where(Book.file_format == "cbz")).first()
        assert book is not None
        assert "comic" in book.title.lower()


@pytest.mark.asyncio
async def test_pdf_ingestion(integration_ctx: IntegrationContext, test_data_dir: Path):
    """E2E Test: PDF Ingestion"""
    ctx = integration_ctx

    test_file = ctx.watch_dir / "beauty.pdf"
    test_file.write_bytes((test_data_dir / "beauty_and_the_beast.pdf").read_bytes())

    from kobosync.scanner import ScannerService

    scanner = ScannerService(settings=ctx.settings, queue=ctx.queue)
    await scanner.scan_directories()

    await wait_for_jobs(ctx.engine)

    with Session(ctx.engine) as session:
        book = session.exec(select(Book).where(Book.file_format == "pdf")).first()
        assert book is not None
        assert book.title is not None
        assert len(book.title) > 0
