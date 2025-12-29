"""Task registry for creating tasks."""

from __future__ import annotations

from typing import TYPE_CHECKING

from .conversion import KepubConverter
from .metadata.manager import MetadataManager
from .organizer import LibraryOrganizer
from .tasks.convert import ConvertTask
from .tasks.ingest import IngestTask
from .tasks.metadata import MetadataTask
from .tasks.organize import OrganizeTask

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

    from .config import Settings
    from .task_queue import TaskQueue
    from .tasks.base import Task


def create_tasks(
    settings: Settings,
    engine: Engine,
    queue: TaskQueue,
) -> dict[str, Task]:
    metadata_manager = MetadataManager(settings)
    converter = KepubConverter()
    organizer = LibraryOrganizer(settings)

    return {
        IngestTask.TASK_TYPE: IngestTask(settings, engine, queue),
        MetadataTask.TASK_TYPE: MetadataTask(settings, engine, metadata_manager, queue),
        ConvertTask.TASK_TYPE: ConvertTask(settings, engine, converter),
        OrganizeTask.TASK_TYPE: OrganizeTask(settings, engine, organizer),
    }
