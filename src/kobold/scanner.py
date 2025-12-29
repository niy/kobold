from __future__ import annotations

from typing import TYPE_CHECKING

from .constants import SUPPORTED_EXTENSIONS
from .logging_config import get_logger
from .tasks.ingest import IngestTask

if TYPE_CHECKING:
    from pathlib import Path

    from .config import Settings
    from .task_queue import TaskQueue

logger = get_logger(__name__)


class ScannerService:
    def __init__(self, settings: Settings, queue: TaskQueue):
        self.settings = settings
        self.task_queue = queue

    async def scan_directories(self, watch_dirs: list[Path] | None = None) -> None:
        logger.info("Starting directory scan...")

        total_count = 0

        target_dirs = watch_dirs or self.settings.watch_dirs_list

        logger.info("Scanning directories", directories=[str(d) for d in target_dirs])

        for watch_dir in target_dirs:
            if not watch_dir.exists():
                logger.warning("Watch directory does not exist", path=str(watch_dir))
                continue

            for file_path in watch_dir.rglob("*"):
                if file_path.is_file() and not file_path.name.startswith("."):
                    ext = file_path.suffix.lower()
                    if ext in SUPPORTED_EXTENSIONS:
                        try:
                            self.task_queue.add_task(
                                task_type=IngestTask.TASK_TYPE,
                                payload={
                                    "event": "ADD",
                                    "path": str(file_path.absolute()),
                                },
                            )
                            total_count += 1
                        except Exception as e:
                            logger.error(
                                "Failed to queue file",
                                path=str(file_path),
                                error=str(e),
                            )

        logger.info("Scan complete", queued_jobs=total_count)
