from unittest.mock import MagicMock, patch

from fastapi.testclient import TestClient
from sqlmodel import Session

from kobosync.main import app


def test_health_check():
    client = TestClient(app)
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


@patch("kobosync.api.health.Session")
def test_readiness_check_ok(mock_session_cls):
    # Mock the session context manager
    mock_session = MagicMock(spec=Session)
    mock_session_cls.return_value.__enter__.return_value = mock_session

    # Mock successful execution
    mock_session.connection.return_value.execute.return_value = None

    client = TestClient(app)
    response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "ready", "database": True}


@patch("kobosync.api.health.Session")
def test_readiness_check_db_failure(mock_session_cls):
    # Mock the session context manager to raise an exception
    mock_session_cls.return_value.__enter__.side_effect = Exception(
        "DB Connection Failed"
    )

    client = TestClient(app)
    response = client.get("/ready")

    assert response.status_code == 200
    assert response.json() == {"status": "degraded", "database": False}
