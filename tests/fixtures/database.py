from collections.abc import Generator
from pathlib import Path
from uuid import uuid4

import pytest
from sqlmodel import Session, SQLModel, create_engine


@pytest.fixture
def temp_db(tmp_path: Path) -> Generator[str]:
    db_path = tmp_path / f"test_{uuid4().hex[:8]}.db"
    db_url = f"sqlite:///{db_path}"

    engine = create_engine(db_url, connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(engine)

    yield db_url

    if db_path.exists():
        db_path.unlink()


@pytest.fixture
def temp_db_session(temp_db: str) -> Generator[Session]:
    engine = create_engine(temp_db, connect_args={"check_same_thread": False})

    with Session(engine) as session:
        yield session
