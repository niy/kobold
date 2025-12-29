import asyncio
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Final

from fastapi import FastAPI

from .api.health import router as health_router
from .api.routes import router as api_router
from .config import get_settings
from .database import create_db_and_tables, engine
from .http_client import HttpClientManager
from .logging_config import configure_logging, get_logger
from .scanner import ScannerService
from .scheduler import schedule_periodic_scans
from .task_queue import TaskQueue
from .task_registry import create_tasks
from .watcher import watch_directories
from .worker import worker

HOST: Final[str] = "0.0.0.0"
PORT: Final[int] = 8000

settings = get_settings()
configure_logging(level=settings.LOG_LEVEL)

logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(_app: FastAPI) -> AsyncGenerator[None]:
    logger.info(
        "Kobold starting",
        version="0.1.0",
        watch_dirs=settings.WATCH_DIRS,
        port=PORT,
    )

    create_db_and_tables()

    queue = TaskQueue(settings, engine)
    tasks = create_tasks(settings, engine, queue)

    worker_task = asyncio.create_task(
        worker(queue, tasks, settings.WORKER_POLL_INTERVAL),
        name="worker",
    )
    logger.info("Worker started")

    watcher_task = asyncio.create_task(
        watch_directories(settings.watch_dirs_list, settings, queue), name="watcher"
    )
    logger.info("File watcher started", directories=settings.WATCH_DIRS)

    scanner_service = ScannerService(settings, queue)

    scan_task = asyncio.create_task(
        scanner_service.scan_directories(), name="initial_scan"
    )
    logger.info("Initial directory scan queued")

    scheduler_task = asyncio.create_task(
        schedule_periodic_scans(scanner_service), name="scheduler"
    )
    logger.info("Scheduler started")

    logger.info(
        "Kobold ready",
        host=HOST,
        port=PORT,
        convert_epub=settings.CONVERT_EPUB,
    )

    yield

    logger.info("Kobold shutting down...")

    scan_task.cancel()
    scheduler_task.cancel()
    watcher_task.cancel()
    worker_task.cancel()

    for task, name in [
        (watcher_task, "Watcher"),
        (worker_task, "Worker"),
        (scheduler_task, "Scheduler"),
    ]:
        try:
            await asyncio.wait_for(task, timeout=5.0)
        except asyncio.CancelledError:
            pass
        except TimeoutError:
            logger.warning(f"{name} task did not stop gracefully")

    logger.debug("Background tasks stopped")

    await HttpClientManager.close()
    logger.debug("HTTP client closed")

    logger.info("Kobold shutdown complete")


app = FastAPI(
    title="Kobold",
    description="Headless Kobo library sync daemon",
    version="0.1.0",
    lifespan=lifespan,
)

app.include_router(health_router)
app.include_router(api_router)


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "kobold.main:app",
        host=HOST,
        port=PORT,
        reload=False,  # Disable for production
        log_level=get_settings().LOG_LEVEL.lower(),
    )
