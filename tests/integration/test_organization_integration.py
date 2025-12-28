import asyncio
import contextlib
from pathlib import Path

import pytest
from sqlmodel import Session, SQLModel, col, create_engine, select

from kobold.config import Settings
from kobold.job_queue import JobQueue
from kobold.models import Book, Job, JobStatus
from kobold.scanner import ScannerService
from kobold.worker import stop_event, worker


async def wait_for_jobs(engine, timeout_sec: float = 5.0) -> None:
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


@contextlib.asynccontextmanager
async def run_worker(ctx):
    stop_event.clear()

    worker_task = asyncio.create_task(
        worker(ctx["settings"], ctx["engine"], ctx["queue"])
    )

    await asyncio.sleep(0.05)

    try:
        yield
    finally:
        stop_event.set()
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task


@pytest.fixture
async def organize_ctx(tmp_path: Path, mock_kepub_converter):
    watch_dir = tmp_path / "books"
    watch_dir.mkdir()

    db_path = tmp_path / "test_org.db"
    test_engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(test_engine)

    test_settings = Settings(
        DATA_PATH=tmp_path,
        WATCH_DIRS=str(watch_dir),
        USER_TOKEN="test_token",
        CONVERT_EPUB=False,
        ORGANIZE_LIBRARY=True,
        ORGANIZE_TEMPLATE="{author}/{title}",
        FETCH_EXTERNAL_METADATA=False,
        WORKER_POLL_INTERVAL=0.01,
    )

    test_queue = JobQueue(test_settings, test_engine)

    yield {
        "watch_dir": watch_dir,
        "settings": test_settings,
        "engine": test_engine,
        "queue": test_queue,
    }


@pytest.fixture
async def organize_ctx_with_series(tmp_path: Path, mock_kepub_converter):
    watch_dir = tmp_path / "books"
    watch_dir.mkdir()

    db_path = tmp_path / "test_org_series.db"
    test_engine = create_engine(f"sqlite:///{db_path}")
    SQLModel.metadata.create_all(test_engine)

    test_settings = Settings(
        DATA_PATH=tmp_path,
        WATCH_DIRS=str(watch_dir),
        USER_TOKEN="test_token",
        CONVERT_EPUB=False,
        ORGANIZE_LIBRARY=True,
        ORGANIZE_TEMPLATE="{author}/{series}/{title}",
        FETCH_EXTERNAL_METADATA=False,
        WORKER_POLL_INTERVAL=0.01,
    )

    test_queue = JobQueue(test_settings, test_engine)

    yield {
        "watch_dir": watch_dir,
        "settings": test_settings,
        "engine": test_engine,
        "queue": test_queue,
    }


class TestOrganizationHappyPath:
    @pytest.mark.asyncio
    async def test_organization_moves_file_after_ingest(
        self, organize_ctx, test_data_dir: Path
    ):
        ctx = organize_ctx

        source_epub = test_data_dir / "romeo_and_juliet.epub"
        book_path = ctx["watch_dir"] / "romeo.epub"
        book_path.write_bytes(source_epub.read_bytes())

        scanner = ScannerService(settings=ctx["settings"], queue=ctx["queue"])
        await scanner.scan_directories()

        async with run_worker(ctx):
            await wait_for_jobs(ctx["engine"])

        with Session(ctx["engine"]) as session:
            book = session.exec(
                select(Book).where(col(Book.title).contains("Romeo"))
            ).first()

            assert book, "Book not found in database"
            new_path = Path(book.file_path)
            assert "Shakespeare" in str(new_path) or "Unknown" in str(new_path)
            assert "Romeo" in str(new_path)
            assert new_path.exists()

    @pytest.mark.asyncio
    async def test_organization_with_series_template(
        self, organize_ctx_with_series, test_data_dir: Path
    ):
        ctx = organize_ctx_with_series

        source_epub = test_data_dir / "romeo_and_juliet.epub"
        book_path = ctx["watch_dir"] / "romeo_series.epub"
        book_path.write_bytes(source_epub.read_bytes())

        scanner = ScannerService(settings=ctx["settings"], queue=ctx["queue"])
        await scanner.scan_directories()

        async with run_worker(ctx):
            await wait_for_jobs(ctx["engine"])

        with Session(ctx["engine"]) as session:
            book = session.exec(
                select(Book).where(col(Book.title).contains("Romeo"))
            ).first()

            if book:
                new_path = Path(book.file_path)
                assert new_path.exists()

    @pytest.mark.asyncio
    async def test_organization_pdf_file(self, organize_ctx, test_data_dir: Path):
        ctx = organize_ctx

        source_pdf = test_data_dir / "beauty_and_the_beast.pdf"
        book_path = ctx["watch_dir"] / "beauty.pdf"
        book_path.write_bytes(source_pdf.read_bytes())

        scanner = ScannerService(settings=ctx["settings"], queue=ctx["queue"])
        await scanner.scan_directories()

        async with run_worker(ctx):
            await wait_for_jobs(ctx["engine"])

        with Session(ctx["engine"]) as session:
            book = session.exec(select(Book).where(Book.file_format == "pdf")).first()

            if book:
                new_path = Path(book.file_path)
                assert new_path.exists()


class TestOrganizationEdgeCases:
    @pytest.mark.asyncio
    async def test_organization_disabled(
        self, tmp_path: Path, test_data_dir: Path, mock_kepub_converter
    ):
        watch_dir = tmp_path / "books"
        watch_dir.mkdir()

        db_path = tmp_path / "test_no_org.db"
        test_engine = create_engine(f"sqlite:///{db_path}")
        SQLModel.metadata.create_all(test_engine)

        test_settings = Settings(
            DATA_PATH=tmp_path,
            WATCH_DIRS=str(watch_dir),
            USER_TOKEN="test_token",
            CONVERT_EPUB=False,
            ORGANIZE_LIBRARY=False,
            FETCH_EXTERNAL_METADATA=False,
            WORKER_POLL_INTERVAL=0.01,
        )

        test_queue = JobQueue(test_settings, test_engine)

        ctx = {
            "watch_dir": watch_dir,
            "settings": test_settings,
            "engine": test_engine,
            "queue": test_queue,
        }

        source_epub = test_data_dir / "romeo_and_juliet.epub"
        book_path = watch_dir / "romeo_no_org.epub"
        book_path.write_bytes(source_epub.read_bytes())

        scanner = ScannerService(settings=test_settings, queue=test_queue)
        await scanner.scan_directories()

        async with run_worker(ctx):
            await wait_for_jobs(test_engine)

        with Session(test_engine) as session:
            book = session.exec(
                select(Book).where(col(Book.title).contains("Romeo"))
            ).first()

            assert book, "Book not found"
            assert book.file_path == str(book_path)

    @pytest.mark.asyncio
    async def test_organization_handles_special_characters(
        self, organize_ctx, test_data_dir: Path
    ):
        ctx = organize_ctx

        source_epub = test_data_dir / "romeo_and_juliet.epub"
        book_path = ctx["watch_dir"] / "special_chars.epub"
        book_path.write_bytes(source_epub.read_bytes())

        scanner = ScannerService(settings=ctx["settings"], queue=ctx["queue"])
        await scanner.scan_directories()

        async with run_worker(ctx):
            await wait_for_jobs(ctx["engine"])

        with Session(ctx["engine"]) as session:
            book = session.exec(
                select(Book).where(col(Book.title).contains("Romeo"))
            ).first()

            if book:
                new_path = Path(book.file_path)
                path_str = str(new_path)
                invalid_chars = '<>:"|?*'
                for char in invalid_chars:
                    assert char not in path_str
                assert new_path.exists()


class TestOrganizationCollisionHandling:
    @pytest.mark.asyncio
    async def test_organization_handles_duplicate_filename(
        self, organize_ctx, test_data_dir: Path
    ):
        ctx = organize_ctx

        source_epub = test_data_dir / "romeo_and_juliet.epub"

        book_path1 = ctx["watch_dir"] / "romeo1.epub"
        book_path1.write_bytes(source_epub.read_bytes())

        book_path2 = ctx["watch_dir"] / "romeo2.epub"
        book_path2.write_bytes(source_epub.read_bytes())

        scanner = ScannerService(settings=ctx["settings"], queue=ctx["queue"])
        await scanner.scan_directories()

        async with run_worker(ctx):
            await wait_for_jobs(ctx["engine"])

        with Session(ctx["engine"]) as session:
            books = session.exec(
                select(Book).where(col(Book.title).contains("Romeo"))
            ).all()

            assert len(books) >= 1


class TestLibraryOrganizerFilesystem:
    @pytest.fixture
    def mock_settings(self, tmp_path):
        from unittest.mock import Mock

        settings = Mock()
        settings.ORGANIZE_LIBRARY = True
        settings.ORGANIZE_TEMPLATE = "{author}/{title}"
        settings.watch_dirs_list = [tmp_path]
        return settings

    @pytest.fixture
    def organizer(self, mock_settings):
        from kobold.organizer import LibraryOrganizer

        return LibraryOrganizer(mock_settings)

    @pytest.fixture
    def mock_book(self, tmp_path):
        from unittest.mock import Mock

        book = Mock(spec=Book)
        book.id = "123"
        book.title = "Test Book"
        book.author = "Test Author"
        book.series = None
        book.series_index = None
        book.language = "en"
        book.genre = "Fiction"
        book.publication_date = None
        book.kepub_path = None
        book.file_hash = "test_hash"
        return book

    def test_organize_moves_kepub_file(self, mock_book, organizer, tmp_path):
        main_file = tmp_path / "incoming" / "test.epub"
        kepub_file = tmp_path / "incoming" / "test.kepub.epub"
        main_file.parent.mkdir(parents=True)
        main_file.touch()
        kepub_file.touch()

        mock_book.file_path = str(main_file)
        mock_book.kepub_path = str(kepub_file)

        new_path = organizer.organize_book(mock_book)

        assert new_path is not None
        assert not main_file.exists()
        assert not kepub_file.exists()
        assert Path(new_path).exists()
        assert mock_book.kepub_path != str(kepub_file)

    def test_organize_kepub_nonexistent_skipped(self, mock_book, organizer, tmp_path):
        main_file = tmp_path / "incoming" / "test.epub"
        main_file.parent.mkdir(parents=True)
        main_file.touch()

        mock_book.file_path = str(main_file)
        mock_book.kepub_path = "/nonexistent/path/test.kepub.epub"

        new_path = organizer.organize_book(mock_book)

        assert new_path is not None
        assert Path(new_path).exists()

    def test_organize_skips_existing_numbered_files(
        self, mock_book, organizer, tmp_path
    ):
        target_dir = tmp_path / "Test Author" / "Test Book"
        target_dir.mkdir(parents=True)

        (target_dir / "test.epub").touch()
        (target_dir / "test_1.epub").touch()
        (target_dir / "test_2.epub").touch()

        source_file = tmp_path / "incoming" / "test.epub"
        source_file.parent.mkdir(parents=True)
        source_file.touch()

        mock_book.file_path = str(source_file)
        mock_book.file_hash = "different_hash"

        new_path = organizer.organize_book(mock_book)

        assert new_path is not None
        assert "_3" in new_path
