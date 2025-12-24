from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlmodel import Session, col, select

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.engine import Engine

    from .config import Settings

from .logging_config import get_logger
from .models import Job, JobStatus, JobType

logger = get_logger(__name__)


JOB_MAX_RETRIES = 3
JOB_STALE_MINUTES = 30


class JobQueue:
    def __init__(self, settings: Settings, engine: Engine):
        self.settings = settings
        self.engine = engine

    def add_job(
        self,
        job_type: JobType,
        payload: dict[str, Any],
        *,
        deduplicate_key: str | None = None,
    ) -> Job | None:
        with Session(self.engine) as session:
            if deduplicate_key:
                existing = session.exec(
                    select(Job)
                    .where(Job.type == job_type)
                    .where(Job.status == JobStatus.PENDING)
                    .where(Job.payload["dedupe_key"].as_string() == deduplicate_key)
                ).first()

                if existing:
                    logger.debug(
                        "Skipping duplicate job",
                        job_type=job_type,
                        dedupe_key=deduplicate_key,
                    )
                    return None

                payload = {**payload, "dedupe_key": deduplicate_key}

            job = Job(
                type=job_type,
                payload=payload,
                status=JobStatus.PENDING,
                max_retries=JOB_MAX_RETRIES,
            )
            session.add(job)
            session.commit()
            session.refresh(job)

            logger.info(
                "Job added to queue",
                job_id=str(job.id),
                job_type=job_type,
            )
            return job

    def fetch_next_job(self) -> Job | None:
        now = datetime.now(UTC)

        with Session(self.engine) as session:
            next_retry_col = col(Job.next_retry_at)
            created_at_col = col(Job.created_at)

            statement = (
                select(Job)
                .where(Job.status == JobStatus.PENDING)
                .where(
                    (next_retry_col == None)  # noqa: E711 SQLAlchemy comparison
                    | (next_retry_col <= now)
                )
                .order_by(
                    next_retry_col.asc().nulls_last(),
                    created_at_col.asc(),
                )
                .limit(1)
            )

            job = session.exec(statement).first()

            if job:
                job.status = JobStatus.PROCESSING
                job.started_at = now
                session.add(job)
                session.commit()
                session.refresh(job)

                logger.debug(
                    "Job claimed for processing",
                    job_id=str(job.id),
                    job_type=job.type,
                    retry_count=job.retry_count,
                )
                return job

            return None

    def complete_job(
        self,
        job_id: UUID,
        *,
        error: str | None = None,
        status: JobStatus | None = None,
    ) -> None:
        with Session(self.engine) as session:
            job = session.get(Job, job_id)
            if not job:
                logger.warning("Attempted to complete unknown job", job_id=str(job_id))
                return

            if status:
                job.status = status
            elif error:
                job.status = JobStatus.FAILED
            else:
                job.status = JobStatus.COMPLETED

            if error:
                job.error_message = error

            job.completed_at = datetime.now(UTC)
            session.add(job)
            session.commit()

            logger.info(
                "Job completed",
                job_id=str(job_id),
                status=job.status,
                error=error[:100] if error else None,
            )

    def retry_job(
        self,
        job_id: UUID,
        error: str,
        *,
        delay_seconds: int | None = None,
    ) -> None:
        with Session(self.engine) as session:
            job = session.get(Job, job_id)
            if not job:
                logger.warning("Attempted to retry unknown job", job_id=str(job_id))
                return

            job.retry_count += 1
            job.error_message = error
            job.status = JobStatus.PENDING

            if delay_seconds is None:
                delay_seconds = 10 * (2 ** (job.retry_count - 1))

            job.next_retry_at = datetime.now(UTC) + timedelta(seconds=delay_seconds)
            session.add(job)
            session.commit()

            logger.warning(
                "Job scheduled for retry",
                job_id=str(job_id),
                retry_count=job.retry_count,
                next_retry_at=job.next_retry_at.isoformat(),
                error=error[:100],
            )

    def recover_stale_jobs(self) -> int:
        cutoff = datetime.now(UTC) - timedelta(minutes=JOB_STALE_MINUTES)

        with Session(self.engine) as session:
            started_at_col = col(Job.started_at)
            stale_jobs = session.exec(
                select(Job)
                .where(Job.status == JobStatus.PROCESSING)
                .where(started_at_col < cutoff)
            ).all()

            for job in stale_jobs:
                job.status = JobStatus.PENDING
                job.started_at = None
                job.retry_count += 1
                job.error_message = "Job recovered from stale state"
                session.add(job)

                logger.warning(
                    "Recovered stale job",
                    job_id=str(job.id),
                    job_type=job.type,
                    was_started_at=job.started_at.isoformat()
                    if job.started_at
                    else None,
                )

            session.commit()

            if stale_jobs:
                logger.info(
                    "Stale job recovery complete",
                    recovered_count=len(stale_jobs),
                )

            return len(stale_jobs)

    def get_queue_stats(self) -> dict[str, int]:
        with Session(self.engine) as session:
            stats = {}
            for status in JobStatus:
                count = session.exec(select(Job).where(Job.status == status)).all()
                stats[status.value] = len(count)
            return stats
