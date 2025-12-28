from datetime import UTC, datetime
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from sqlmodel import JSON, Field, SQLModel

# ─────────────────────────────────────────────────────────────────────────────
# Job Queue Models
# ─────────────────────────────────────────────────────────────────────────────


class JobStatus(str, Enum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    DEAD_LETTER = "DEAD_LETTER"


class JobType(str, Enum):
    INGEST = "INGEST"
    CONVERT = "CONVERT"
    METADATA = "METADATA"
    ORGANIZE = "ORGANIZE"


class Job(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    type: JobType = Field(index=True)
    payload: dict[str, Any] = Field(default_factory=dict, sa_type=JSON)
    status: JobStatus = Field(default=JobStatus.PENDING, index=True)
    error_message: str | None = None

    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC), index=True)
    started_at: datetime | None = None
    completed_at: datetime | None = None

    retry_count: int = Field(default=0)
    max_retries: int = Field(default=3)
    next_retry_at: datetime | None = Field(default=None, index=True)

    def __repr__(self) -> str:
        return f"Job(id={self.id!s:.8}, type={self.type}, status={self.status})"


# ─────────────────────────────────────────────────────────────────────────────
# Book Models
# ─────────────────────────────────────────────────────────────────────────────


class BookBase(SQLModel):
    title: str = Field(index=True)
    author: str | None = Field(default=None, index=True)
    isbn: str | None = Field(default=None, index=True)
    description: str | None = None

    language: str | None = None
    publisher: str | None = None
    subtitle: str | None = None
    series: str | None = Field(default=None, index=True)
    series_index: float | None = None
    series_total: int | None = None
    rating: float | None = None
    review_count: int | None = None
    publication_date: datetime | None = None
    genre: str | None = Field(default=None, index=True)

    file_path: str = Field(unique=True, index=True)
    kepub_path: str | None = None
    cover_path: str | None = None
    file_hash: str = Field(index=True)
    file_size: int | None = None
    file_format: str | None = None

    is_converted: bool = False
    is_deleted: bool = Field(default=False, index=True)
    deleted_at: datetime | None = None

    added_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime = Field(default_factory=lambda: datetime.now(UTC))


class Book(BookBase, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)

    def __repr__(self) -> str:
        return f"Book(id={self.id!s:.8}, title={self.title!r}, author={self.author!r})"

    def mark_updated(self) -> None:
        self.updated_at = datetime.now(UTC)

    def mark_deleted(self) -> None:
        self.is_deleted = True
        self.deleted_at = datetime.now(UTC)
        self.mark_updated()


class BookCreate(BookBase):
    pass


class BookUpdate(SQLModel):
    title: str | None = None
    author: str | None = None
    description: str | None = None
    cover_path: str | None = None
    rating: float | None = None
    series: str | None = None
    series_index: float | None = None


# ─────────────────────────────────────────────────────────────────────────────
# Reading State Models
# ─────────────────────────────────────────────────────────────────────────────


class ReadingStatus(str, Enum):
    UNREAD = "Unread"
    READING = "Reading"
    FINISHED = "Finished"


class ReadingState(SQLModel, table=True):
    id: UUID = Field(default_factory=uuid4, primary_key=True)
    book_id: UUID = Field(index=True, foreign_key="book.id", unique=True)

    progress_percent: int = Field(default=0, ge=0, le=100)

    status: str = Field(default=ReadingStatus.UNREAD.value)
    last_modified: datetime = Field(default_factory=lambda: datetime.now(UTC))

    location_value: str | None = None  # CFI or chapter ID
    location_type: str | None = None
    location_source: str | None = None
    spent_reading_minutes: int = Field(default=0, ge=0)
    remaining_time_minutes: int = Field(default=0, ge=0)

    last_finished: datetime | None = None
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))

    def __repr__(self) -> str:
        return f"ReadingState(book_id={self.book_id!s:.8}, status={self.status}, progress={self.progress_percent}%)"

    def mark_updated(self) -> None:
        self.last_modified = datetime.now(UTC)
