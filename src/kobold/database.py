from collections.abc import Generator
from contextlib import contextmanager
from typing import TYPE_CHECKING

from sqlmodel import Session, SQLModel, create_engine

from .config import get_settings
from .logging_config import get_logger

# Import models to register them with SQLModel metadata
from .models import Book, ReadingState, Task  # noqa: F401

if TYPE_CHECKING:
    from sqlalchemy.engine import Engine

logger = get_logger(__name__)


def _create_engine() -> Engine:
    connect_args = {
        "check_same_thread": False,
        "timeout": 30,
    }

    return create_engine(
        get_settings().db_url,
        echo=False,
        connect_args=connect_args,
        pool_pre_ping=True,
    )


engine = _create_engine()


def create_db_and_tables() -> None:
    logger.info("Initializing database", db_url=get_settings().db_url)
    SQLModel.metadata.create_all(engine)
    logger.info("Database initialized successfully")


@contextmanager
def get_session() -> Generator[Session]:
    session = Session(engine)
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        raise
    finally:
        session.close()


def get_session_dependency() -> Generator[Session]:
    with get_session() as session:
        yield session
