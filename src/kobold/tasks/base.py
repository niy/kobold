"""Base class for tasks."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, ClassVar


class Task(ABC):
    """Base class for tasks.

    Each task must define a TASK_TYPE class attribute that identifies
    which task type it processes. This is enforced at class definition time.

    Example:
        class MyTask(Task):
            TASK_TYPE = "MY_TASK"

            async def process(self, payload: dict) -> None:
                ...
    """

    TASK_TYPE: ClassVar[str]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Validate TASK_TYPE at class definition time."""
        super().__init_subclass__(**kwargs)

        # Skip validation for abstract base classes
        if ABC in cls.__bases__:
            return

        # Enforce TASK_TYPE
        if not hasattr(cls, "TASK_TYPE") or not cls.TASK_TYPE:
            raise TypeError(
                f"{cls.__name__} must define a non-empty TASK_TYPE class attribute"
            )

    @abstractmethod
    async def process(self, payload: dict[str, Any]) -> None:
        """Process a task with the given payload."""
        ...
