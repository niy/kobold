from collections.abc import Generator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from kobosync.config import Settings, get_settings
from kobosync.main import app


@pytest.fixture
def client(tmp_path: Path) -> Generator[TestClient]:
    def get_test_settings() -> Settings:
        return Settings(DATA_PATH=tmp_path, USER_TOKEN="test_token")

    app.dependency_overrides[get_settings] = get_test_settings

    with TestClient(app) as client:
        yield client

    app.dependency_overrides.clear()
