from __future__ import annotations

import asyncio
import threading
from typing import TYPE_CHECKING

from .conversion import KepubConverter
from .logging_config import get_logger
from .metadata.manager import MetadataManager
from .models import JobStatus, JobType
from .services.conversion_service import ConversionJobService
from .services.ingest import IngestService
from .services.metadata_service import MetadataJobService

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from .config import Settings
    from .job_queue import JobQueue

logger = get_logger(__name__)
stop_event = threading.Event()

# Interval between polling for new jobs
WORKER_POLL_INTERVAL = 300.0
# Backoff interval after a critical worker loop error
WORKER_ERROR_BACKOFF = 5.0


async def worker(
    settings_obj: Settings,
    db_engine: Engine,
    queue: JobQueue,
) -> None:
    from .job_queue import JOB_MAX_RETRIES

    logger.info(
        "Worker starting",
        poll_interval=WORKER_POLL_INTERVAL,
        max_retries=JOB_MAX_RETRIES,
    )

    metadata_manager = MetadataManager(settings_obj)
    converter = KepubConverter()

    ingest_service = IngestService(settings_obj, db_engine, queue)
    metadata_service = MetadataJobService(settings_obj, db_engine, metadata_manager)
    conversion_service = ConversionJobService(settings_obj, db_engine, converter)

    try:
        recovered = queue.recover_stale_jobs()
        if recovered:
            logger.info("Recovered stale jobs", count=recovered)
    except Exception as e:
        logger.error("Failed to recover stale jobs", error=str(e))

    logger.info("Worker services initialized")

    while not stop_event.is_set():
        job = None
        try:
            job = queue.fetch_next_job()

            if not job:
                await asyncio.sleep(WORKER_POLL_INTERVAL)
                continue

            log = logger.bind(
                job_id=str(job.id),
                job_type=job.type.value,
                retry_count=job.retry_count,
            )
            log.info("Processing job")

            try:
                match job.type:
                    case JobType.INGEST:
                        await ingest_service.process_job(job.payload)
                    case JobType.METADATA:
                        await metadata_service.process_job(job.payload)
                    case JobType.CONVERT:
                        await conversion_service.process_job(job.payload)
                    case _:
                        logger.error("Unknown job type", job_type=job.type)
                        queue.complete_job(
                            job.id,
                            error=f"Unknown job type: {job.type}",
                            status=JobStatus.FAILED,
                        )
                        continue

                queue.complete_job(job.id)
                log.info("Job completed successfully")

            except Exception as e:
                error_msg = f"{type(e).__name__}: {e}"
                log.error("Job failed", error=error_msg, exc_info=True)

                if job.retry_count < job.max_retries:
                    queue.retry_job(job.id, error_msg)
                else:
                    log.error("Job permanently failed, moving to dead letter")
                    queue.complete_job(
                        job.id,
                        error=error_msg,
                        status=JobStatus.DEAD_LETTER,
                    )

        except asyncio.CancelledError:
            logger.info("Worker received cancellation, shutting down gracefully")
            break

        except Exception as e:
            if job:
                logger.error(
                    "Job failed due to worker loop error",
                    job_id=str(job.id),
                    error=str(e),
                    exc_info=True,
                )

                if job.retry_count < job.max_retries:
                    queue.retry_job(job.id, str(e))
                else:
                    queue.complete_job(
                        job.id,
                        error=str(e),
                        status=JobStatus.DEAD_LETTER,
                    )
            else:
                logger.error(
                    "Critical worker loop error (no job context)",
                    error=str(e),
                    exc_info=True,
                )

            await asyncio.sleep(WORKER_ERROR_BACKOFF)

    logger.info("Worker stopped")
