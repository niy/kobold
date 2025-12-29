"""Worker loop that processes tasks from the queue."""

from __future__ import annotations

import asyncio
import contextlib
from typing import TYPE_CHECKING

from .logging_config import get_logger
from .models import TaskStatus

if TYPE_CHECKING:
    from .models import Task as TaskModel
    from .task_queue import TaskQueue
    from .tasks.base import Task

logger = get_logger(__name__)

WORKER_ERROR_BACKOFF = 5.0


def _handle_task_failure(queue: TaskQueue, task: TaskModel, error_msg: str) -> None:
    """Handle task failure with retry or dead-letter."""
    if task.retry_count < task.max_retries:
        queue.retry_task(task.id, error_msg)
    else:
        logger.error(
            "Task permanently failed, moving to dead letter", task_id=str(task.id)
        )
        queue.complete_task(task.id, error=error_msg, status=TaskStatus.DEAD_LETTER)


async def _process_task(
    task: TaskModel,
    tasks: dict[str, Task],
    queue: TaskQueue,
) -> None:
    """Process a single task using the appropriate task processor."""
    task_type = task.type

    log = logger.bind(
        task_id=str(task.id),
        task_type=task_type,
        retry_count=task.retry_count,
    )
    log.info("Processing task")

    processor = tasks.get(task_type)
    if not processor:
        error_msg = f"Unknown task type: {task_type}"
        log.error("Unknown task type")
        queue.complete_task(task.id, error=error_msg, status=TaskStatus.FAILED)
        return

    try:
        await processor.process(task.payload)
        queue.complete_task(task.id)
        log.info("Task completed successfully")
    except Exception as e:
        error_msg = f"{type(e).__name__}: {e}"
        log.error("Task failed", error=error_msg, exc_info=True)
        _handle_task_failure(queue, task, error_msg)


async def _wait_for_task(queue: TaskQueue, wait_timeout: float) -> TaskModel | None:
    """Wait for a task, returning None if timeout or no task available."""
    task = queue.fetch_next_task()
    if task:
        return task

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(queue.task_event.wait(), timeout=wait_timeout)
    queue.task_event.clear()
    return None


async def worker(
    queue: TaskQueue,
    tasks: dict[str, Task],
    poll_interval: float,
) -> None:
    """Main worker loop that processes tasks from the queue.

    Args:
        queue: Task queue for fetching and managing tasks.
        tasks: Registry mapping task type strings to task processor instances.
        poll_interval: Seconds to wait for new tasks before checking again.
    """
    from .task_queue import TASK_MAX_RETRIES

    logger.info(
        "Worker starting",
        poll_interval=poll_interval,
        max_retries=TASK_MAX_RETRIES,
        task_types=list(tasks.keys()),
    )

    try:
        recovered = queue.recover_stale_tasks()
        if recovered:
            logger.info("Recovered stale tasks", count=recovered)
    except Exception as e:
        logger.error("Failed to recover stale tasks", error=str(e))

    logger.info("Worker ready")

    try:
        while True:
            try:
                task = await _wait_for_task(queue, poll_interval)
                if task:
                    await _process_task(task, tasks, queue)

            except asyncio.CancelledError:
                raise

            except Exception as e:
                logger.error("Worker loop error", error=str(e), exc_info=True)
                await asyncio.sleep(WORKER_ERROR_BACKOFF)

    except asyncio.CancelledError:
        logger.info("Worker stopped")
