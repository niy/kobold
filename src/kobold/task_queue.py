from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from sqlmodel import Session, col, select

if TYPE_CHECKING:
    from uuid import UUID

    from sqlalchemy.engine import Engine

    from .config import Settings

from .logging_config import get_logger
from .models import Task, TaskStatus

logger = get_logger(__name__)


TASK_MAX_RETRIES = 3
TASK_STALE_MINUTES = 15


class TaskQueue:
    def __init__(self, settings: Settings, engine: Engine):
        self.settings = settings
        self.engine = engine
        self._task_event: asyncio.Event | None = None

    @property
    def task_event(self) -> asyncio.Event:
        """Get or create the task available event (lazy initialization)."""
        if self._task_event is None:
            self._task_event = asyncio.Event()
        return self._task_event

    def notify(self) -> None:
        """Signal that a task is available for processing."""
        self.task_event.set()

    def add_task(
        self,
        task_type: str,
        payload: dict[str, Any],
    ) -> Task:
        with Session(self.engine) as session:
            task = Task(
                type=task_type,
                payload=payload,
                status=TaskStatus.PENDING,
                max_retries=TASK_MAX_RETRIES,
            )
            session.add(task)
            session.commit()
            session.refresh(task)

            logger.info(
                "Task added to queue",
                task_id=str(task.id),
                task_type=task_type,
            )

            self.notify()
            return task

    def fetch_next_task(self) -> Task | None:
        now = datetime.now(UTC)

        with Session(self.engine) as session:
            next_retry_col = col(Task.next_retry_at)
            created_at_col = col(Task.created_at)

            statement = (
                select(Task)
                .where(Task.status == TaskStatus.PENDING)
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

            task = session.exec(statement).first()

            if task:
                task.status = TaskStatus.PROCESSING
                task.started_at = now
                session.add(task)
                session.commit()
                session.refresh(task)

                logger.debug(
                    "Task claimed for processing",
                    task_id=str(task.id),
                    task_type=task.type,
                    retry_count=task.retry_count,
                )
                return task

            return None

    def complete_task(
        self,
        task_id: UUID,
        *,
        error: str | None = None,
        status: TaskStatus | None = None,
    ) -> None:
        with Session(self.engine) as session:
            task = session.get(Task, task_id)
            if not task:
                logger.warning(
                    "Attempted to complete unknown task", task_id=str(task_id)
                )
                return

            if status:
                task.status = status
            elif error:
                task.status = TaskStatus.FAILED
            else:
                task.status = TaskStatus.COMPLETED

            if error:
                task.error_message = error

            task.completed_at = datetime.now(UTC)
            session.add(task)
            session.commit()

            logger.info(
                "Task completed",
                task_id=str(task_id),
                status=task.status,
                error=error[:100] if error else None,
            )

    def retry_task(
        self,
        task_id: UUID,
        error: str,
        *,
        delay_seconds: int | None = None,
    ) -> None:
        with Session(self.engine) as session:
            task = session.get(Task, task_id)
            if not task:
                logger.warning("Attempted to retry unknown task", task_id=str(task_id))
                return

            task.retry_count += 1
            task.error_message = error
            task.status = TaskStatus.PENDING

            if delay_seconds is None:
                delay_seconds = 10 * (2 ** (task.retry_count - 1))

            task.next_retry_at = datetime.now(UTC) + timedelta(seconds=delay_seconds)
            session.add(task)
            session.commit()

            logger.warning(
                "Task scheduled for retry",
                task_id=str(task_id),
                retry_count=task.retry_count,
                next_retry_at=task.next_retry_at.isoformat(),
                error=error[:100],
            )

    def recover_stale_tasks(self) -> int:
        cutoff = datetime.now(UTC) - timedelta(minutes=TASK_STALE_MINUTES)

        with Session(self.engine) as session:
            started_at_col = col(Task.started_at)
            stale_tasks = session.exec(
                select(Task)
                .where(Task.status == TaskStatus.PROCESSING)
                .where(started_at_col < cutoff)
            ).all()

            for task in stale_tasks:
                task.status = TaskStatus.PENDING
                task.started_at = None
                task.retry_count += 1
                task.error_message = "Task recovered from stale state"
                session.add(task)

                logger.warning(
                    "Recovered stale task",
                    task_id=str(task.id),
                    task_type=task.type,
                    was_started_at=task.started_at.isoformat()
                    if task.started_at
                    else None,
                )

            session.commit()

            if stale_tasks:
                logger.info(
                    "Stale task recovery complete",
                    recovered_count=len(stale_tasks),
                )

            return len(stale_tasks)

    def get_queue_stats(self) -> dict[str, int]:
        with Session(self.engine) as session:
            stats = {}
            for status in TaskStatus:
                count = session.exec(select(Task).where(Task.status == status)).all()
                stats[status.value] = len(count)
            return stats
