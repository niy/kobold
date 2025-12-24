"""
Async file watcher using watchfiles.

Monitors directories for file changes and enqueues jobs for processing.
Uses the Rust-based watchfiles library for efficient, event-driven monitoring.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import TYPE_CHECKING

from watchfiles import Change, DefaultFilter, awatch

from .constants import SUPPORTED_EXTENSIONS
from .logging_config import get_logger
from .models import JobType

if TYPE_CHECKING:
    from .config import Settings
    from .job_queue import JobQueue

logger = get_logger(__name__)

# Wait this long (ms) to group events before yielding
WATCH_DEBOUNCE_MS = 1600


class BookFilter(DefaultFilter):
    """
    Custom filter for book files.

    Extends DefaultFilter to only watch for supported book formats.
    This filters at the Rust level for better performance.
    """

    def __call__(self, change: Change, path: str) -> bool:
        if not super().__call__(change, path):
            return False

        p = Path(path)
        if p.name.startswith("."):
            return False

        return p.suffix.lower() in SUPPORTED_EXTENSIONS


async def watch_directories(
    watch_dirs: list[Path],
    settings: Settings,
    queue: JobQueue,
) -> None:
    """
    Watch directories for file changes and enqueue jobs.

    Uses watchfiles.awatch for efficient async monitoring with built-in
    debouncing. Supports polling mode for network shares (NFS/SMB).

    Args:
        watch_dirs: Directories to monitor
        settings: Application settings
        queue: Job queue for enqueuing ingest tasks
    """
    dirs_to_watch = []
    for watch_dir in watch_dirs:
        if not watch_dir.exists():
            try:
                watch_dir.mkdir(parents=True, exist_ok=True)
            except Exception as e:
                logger.error(
                    "Failed to create watch directory",
                    path=str(watch_dir),
                    error=str(e),
                )
                continue
        dirs_to_watch.append(watch_dir)

    if not dirs_to_watch:
        logger.warning("No valid directories to watch")
        return

    mode = "polling" if settings.WATCH_FORCE_POLLING else "native"
    logger.info(
        "Starting file watcher",
        mode=mode,
        poll_delay_ms=settings.WATCH_POLL_DELAY_MS
        if settings.WATCH_FORCE_POLLING
        else None,
        directories=[str(d) for d in dirs_to_watch],
    )

    try:
        async for changes in awatch(
            *dirs_to_watch,
            watch_filter=BookFilter(),
            debounce=WATCH_DEBOUNCE_MS,
            force_polling=settings.WATCH_FORCE_POLLING,
            poll_delay_ms=settings.WATCH_POLL_DELAY_MS,
            recursive=True,
            ignore_permission_denied=True,
        ):
            for change_type, path_str in changes:
                if change_type == Change.added:
                    event = "ADD"
                elif change_type == Change.modified:
                    event = "MODIFIED"
                elif change_type == Change.deleted:
                    event = "DELETE"
                else:
                    continue

                logger.info(
                    "File event detected",
                    event_type=event,
                    path=path_str,
                )

                queue.add_job(
                    job_type=JobType.INGEST,
                    payload={
                        "event": event,
                        "path": path_str,
                    },
                )

    except asyncio.CancelledError:
        logger.debug("File watcher cancelled")
        raise
