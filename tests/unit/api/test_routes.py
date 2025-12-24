from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import UUID, uuid4

import pytest
from fastapi.testclient import TestClient

from kobosync.api.proxy import KoboProxyService
from kobosync.config import Settings
from kobosync.models import Book, ReadingState
from kobosync.utils.kobo_token import KoboSyncToken


@pytest.fixture
def test_settings() -> Settings:
    return Settings(
        USER_TOKEN="test_token",
    )


@pytest.fixture
def app_client(test_settings: Settings, temp_db: str) -> TestClient:
    from fastapi import FastAPI
    from sqlmodel import Session, SQLModel, create_engine

    from kobosync.api.routes import router
    from kobosync.config import get_settings
    from kobosync.database import get_session_dependency

    db_url = str(temp_db)
    test_engine = create_engine(db_url, connect_args={"check_same_thread": False})
    SQLModel.metadata.create_all(test_engine)

    test_app = FastAPI()
    test_app.include_router(router)

    test_app.dependency_overrides[get_settings] = lambda: test_settings

    def get_test_session():
        with Session(test_engine) as session:
            yield session

    test_app.dependency_overrides[get_session_dependency] = get_test_session

    return TestClient(test_app)


class TestInitialization:
    def test_initialization_success(self, app_client: TestClient) -> None:
        response = app_client.get("/api/kobo/test_token/v1/initialization")

        assert response.status_code == 200
        data = response.json()
        assert "Resources" in data
        assert "image_host" in data["Resources"]
        assert "image_url_template" in data["Resources"]

    def test_initialization_invalid_token(self, app_client: TestClient) -> None:
        response = app_client.get("/api/kobo/wrong_token/v1/initialization")

        assert response.status_code == 401

    def test_initialization_custom_host(self, app_client: TestClient) -> None:
        custom_host = "kobosync.internal:1234"
        response = app_client.get(
            "/api/kobo/test_token/v1/initialization",
            headers={"Host": custom_host},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["Resources"]["image_host"] == f"http://{custom_host}/images"
        assert (
            data["Resources"]["image_url_template"]
            == f"http://{custom_host}/images/{{ImageId}}/{{Width}}/{{Height}}/False/img.jpg"
        )

    def test_catch_all_proxy(self, app_client: TestClient) -> None:
        mock_proxy = AsyncMock(spec=KoboProxyService)
        from fastapi import FastAPI, Response

        mock_proxy.proxy_request.return_value = Response(
            content=b'{"proxied": true}',
            status_code=201,
            headers={"X-Custom-Header": "test-value"},
        )

        app = app_client.app
        assert isinstance(app, FastAPI)
        app.dependency_overrides[KoboProxyService] = lambda: mock_proxy

        try:
            response = app_client.get("/api/kobo/test_token/some/unknown/path")

            assert response.status_code == 201
            assert response.json() == {"proxied": True}
            assert response.headers["X-Custom-Header"] == "test-value"
            mock_proxy.proxy_request.assert_called_once()
            args, _ = mock_proxy.proxy_request.call_args
            assert args[1] == "/some/unknown/path"
        finally:
            app.dependency_overrides.pop(KoboProxyService, None)


class TestAuthDevice:
    def test_auth_device_success(self, app_client: TestClient) -> None:
        response = app_client.post(
            "/api/kobo/test_token/v1/auth/device",
            json={"UserKey": "test-user"},
        )

        assert response.status_code == 200
        data = response.json()
        assert "AccessToken" in data
        assert "RefreshToken" in data
        assert "UserKey" in data
        assert data["UserKey"] == "test-user"

    def test_auth_device_empty_body(self, app_client: TestClient) -> None:
        response = app_client.post(
            "/api/kobo/test_token/v1/auth/device",
            content="",
        )

        assert response.status_code == 200
        data = response.json()
        assert data["UserKey"] == "local-user"


class TestLibrarySync:
    def test_sync_empty_library(self, app_client: TestClient) -> None:
        response = app_client.get("/api/kobo/test_token/v1/library/sync")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert "X-Kobo-SyncToken" in response.headers

    def test_sync_returns_sync_token(self, app_client: TestClient) -> None:
        response = app_client.get("/api/kobo/test_token/v1/library/sync")

        assert response.status_code == 200
        assert "X-Kobo-SyncToken" in response.headers
        token = response.headers["X-Kobo-SyncToken"]
        assert len(token) > 0

    def test_sync_custom_host(self, app_client: TestClient, temp_db: str) -> None:
        from sqlmodel import Session, create_engine

        from kobosync.models import Book

        engine = create_engine(temp_db, connect_args={"check_same_thread": False})
        book_id = uuid4()
        with Session(engine) as session:
            book = Book(
                id=book_id,
                title="Dynamic Host Test",
                file_path="/tmp/test.epub",
                updated_at=datetime.now(UTC),
                file_hash="hash1",
            )
            session.add(book)
            session.commit()

        custom_host = "my-kobo-server:8888"
        response = app_client.get(
            "/api/kobo/test_token/v1/library/sync",
            headers={"Host": custom_host},
        )

        assert response.status_code == 200
        data = response.json()

        book_entitlement = next(
            (e["NewEntitlement"] for e in data if "NewEntitlement" in e),
            None,
        )
        assert book_entitlement is not None
        expected_base = f"http://{custom_host}/download/{book_id}"
        assert book_entitlement["URL"] == expected_base
        assert book_entitlement["DownloadUrl"] == expected_base
        assert book_entitlement["ProductUrl"] == expected_base

    def test_sync_library_invalid_timestamp(self, app_client: TestClient) -> None:
        token_str = KoboSyncToken(lastSuccessfulSyncPointId="invalid-date").to_base64()

        with patch("kobosync.api.routes.logger") as mock_logger:
            response = app_client.get(
                "/api/kobo/test_token/v1/library/sync",
                headers={"X-Kobo-SyncToken": token_str},
            )
            assert response.status_code == 200
            mock_logger.bind.return_value.warning.assert_called_with(
                "Invalid sync timestamp", value="invalid-date"
            )

    def test_sync_library_proxy_token_update(self, app_client: TestClient) -> None:
        mock_proxy = AsyncMock(spec=KoboProxyService)
        from fastapi import FastAPI

        mock_proxy.fetch_kobo_sync.return_value = (
            200,
            {
                "X-Kobo-SyncToken": KoboSyncToken(
                    rawKoboSyncToken="new-remote-token"
                ).to_base64()
            },
            [],
        )
        app = app_client.app
        assert isinstance(app, FastAPI)
        app.dependency_overrides[KoboProxyService] = lambda: mock_proxy

        try:
            response = app_client.get("/api/kobo/test_token/v1/library/sync")
            assert response.status_code == 200

            new_token_str = response.headers["X-Kobo-SyncToken"]
            new_token = KoboSyncToken.from_base64(new_token_str)
            assert new_token.rawKoboSyncToken == "new-remote-token"
        finally:
            app.dependency_overrides.pop(KoboProxyService, None)

    def test_sync_library_headers_proxying(self, app_client: TestClient) -> None:
        mock_proxy = AsyncMock(spec=KoboProxyService)
        from fastapi import FastAPI

        mock_proxy.fetch_kobo_sync.return_value = (
            200,
            {"X-Kobo-Sync": "some-value"},
            [],
        )
        app = app_client.app
        assert isinstance(app, FastAPI)
        app.dependency_overrides[KoboProxyService] = lambda: mock_proxy

        try:
            response = app_client.get("/api/kobo/test_token/v1/library/sync")
            assert response.status_code == 200
            assert response.headers["X-Kobo-Sync"] == "some-value"
        finally:
            app.dependency_overrides.pop(KoboProxyService, None)


class TestDownload:
    def test_download_nonexistent_book(self, app_client: TestClient) -> None:
        book_id = str(uuid4())
        response = app_client.get(f"/download/{book_id}")

        assert response.status_code == 404

    def test_download_invalid_id_format(self, app_client: TestClient) -> None:
        response = app_client.get("/download/not-a-uuid")

        assert response.status_code == 404

    def test_download_book_missing_file_on_disk(
        self, app_client: TestClient, temp_db: str
    ) -> None:
        from sqlmodel import Session, create_engine

        engine = create_engine(temp_db, connect_args={"check_same_thread": False})

        book_id = str(uuid4())
        with Session(engine) as session:
            book = Book(
                id=UUID(book_id),
                title="Test",
                file_path="/tmp/missing.epub",
                file_hash="hash",
                file_size=100,
            )
            session.add(book)
            session.commit()

        with patch("pathlib.Path.exists", return_value=False):
            response = app_client.get(f"/download/{book_id}")
            assert response.status_code == 404
            assert response.json()["detail"] == "File not found"


class TestCoverImages:
    def test_cover_local_file_success(
        self, app_client: TestClient, temp_db: str, tmp_path: Path
    ) -> None:
        cover_file = tmp_path / "cover.jpg"
        cover_file.write_bytes(b"fake image data")

        from sqlmodel import Session, create_engine

        from kobosync.models import Book

        engine = create_engine(temp_db, connect_args={"check_same_thread": False})

        book_id = str(uuid4())
        with Session(engine) as session:
            book = Book(
                id=UUID(book_id),
                title="Cover Test",
                file_path=f"/tmp/book_{book_id}.epub",
                cover_path=str(cover_file),
                updated_at=datetime.now(UTC),
                file_hash="dummy_hash",
            )
            session.add(book)
            session.commit()

        response = app_client.get(f"/images/{book_id}/200/300/False/img.jpg")

        assert response.status_code == 200
        assert response.content == b"fake image data"

    def test_cover_nonexistent_book(self, app_client: TestClient) -> None:
        book_id = str(uuid4())
        response = app_client.get(f"/images/{book_id}/200/300/False/img.jpg")

        assert response.status_code == 404
        assert response.json()["detail"] == "Cover not found"

    def test_cover_file_missing(
        self, app_client: TestClient, temp_db: str, tmp_path: Path
    ) -> None:
        from sqlmodel import Session, create_engine

        from kobosync.models import Book

        engine = create_engine(temp_db, connect_args={"check_same_thread": False})
        book_id = str(uuid4())
        missing_file = tmp_path / "does_not_exist.jpg"

        with Session(engine) as session:
            book = Book(
                id=UUID(book_id),
                title="Missing Cover",
                file_path=f"/tmp/book_{book_id}.epub",
                cover_path=str(missing_file),
                updated_at=datetime.now(UTC),
                file_hash="dummy_hash",
            )
            session.add(book)
            session.commit()

        response = app_client.get(f"/images/{book_id}/200/300/False/img.jpg")

        assert response.status_code == 404
        assert response.json()["detail"] == "Cover file not found"

    def test_cover_remote_url_success(
        self, app_client: TestClient, temp_db: str
    ) -> None:
        from sqlmodel import Session, create_engine

        from kobosync.models import Book

        engine = create_engine(temp_db, connect_args={"check_same_thread": False})
        book_id = str(uuid4())
        remote_url = "http://example.com/cover.jpg"

        with Session(engine) as session:
            book = Book(
                id=UUID(book_id),
                title="Remote Cover",
                file_path=f"/tmp/book_{book_id}.epub",
                cover_path=remote_url,
                updated_at=datetime.now(UTC),
                file_hash="dummy_hash",
            )
            session.add(book)
            session.commit()

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.headers = {"content-type": "image/jpeg"}

        async def iter_bytes():
            yield b"remote "
            yield b"image"

        mock_response.iter_bytes = iter_bytes

        mock_client = AsyncMock()
        mock_client.get = AsyncMock(return_value=mock_response)

        with patch(
            "kobosync.api.routes.HttpClientManager.get_client",
            return_value=mock_client,
        ):
            response = app_client.get(f"/images/{book_id}/200/300/False/img.jpg")

        assert response.status_code == 200
        assert b"remote image" in response.content

    def test_cover_invalid_id_format(self, app_client: TestClient) -> None:
        response = app_client.get("/images/not-a-uuid/200/300/False/img.jpg")

        assert response.status_code == 404

    def test_get_cover_remote_fetch_failure(
        self, app_client: TestClient, temp_db: str
    ) -> None:
        from sqlmodel import Session, create_engine

        engine = create_engine(temp_db, connect_args={"check_same_thread": False})

        book_id = str(uuid4())
        with Session(engine) as session:
            book = Book(
                id=UUID(book_id),
                title="Test",
                cover_path="http://example.com/cover.jpg",
                file_path="/tmp/test.epub",
                file_hash="hash",
                file_size=100,
            )
            session.add(book)
            session.commit()

        mock_client = AsyncMock()
        mock_client.get.side_effect = Exception("Network error")

        with patch(
            "kobosync.http_client.HttpClientManager.get_client",
            return_value=mock_client,
        ):
            response = app_client.get(f"/images/{book_id}/100/100/False/img.jpg")
            assert response.status_code == 404
            assert response.json()["detail"] == "Cover unavailable"


class TestReadingState:
    def test_get_state_nonexistent_book(self, app_client: TestClient) -> None:
        book_id = str(uuid4())
        response = app_client.get(f"/api/kobo/test_token/v1/library/{book_id}/state")

        assert response.status_code == 200
        data = response.json()
        assert isinstance(data, list)
        assert len(data) == 1
        assert data[0]["StatusInfo"]["Status"] == "Unread"

    def test_update_state_no_body(self, app_client: TestClient) -> None:
        book_id = str(uuid4())
        response = app_client.put(
            f"/api/kobo/test_token/v1/library/{book_id}/state",
            content="",
        )

        assert response.status_code == 400

    def test_update_state_empty_states(self, app_client: TestClient) -> None:
        book_id = str(uuid4())
        response = app_client.put(
            f"/api/kobo/test_token/v1/library/{book_id}/state",
            json={"ReadingStates": []},
        )

        assert response.status_code == 400

    def test_get_reading_state_proxy(self, app_client: TestClient) -> None:
        mock_proxy = AsyncMock(spec=KoboProxyService)
        from fastapi import FastAPI, Response

        mock_proxy.proxy_request.return_value = Response(
            content=b'[{"Status": "Read"}]', status_code=200
        )

        app = app_client.app
        assert isinstance(app, FastAPI)
        app.dependency_overrides[KoboProxyService] = lambda: mock_proxy

        try:
            response = app_client.get("/api/kobo/test_token/v1/library/remote-id/state")

            assert response.status_code == 200
            assert response.json() == [{"Status": "Read"}]

            mock_proxy.proxy_request.assert_called_once()
            args, _ = mock_proxy.proxy_request.call_args
            assert args[1] == "/v1/library/remote-id/state"
        finally:
            app.dependency_overrides.pop(KoboProxyService, None)

    def test_update_reading_state_proxy(self, app_client: TestClient) -> None:
        mock_proxy = AsyncMock(spec=KoboProxyService)
        from fastapi import Response

        mock_proxy.proxy_request.return_value = Response(
            content=b'{"updated": true}', status_code=200
        )

        from fastapi import FastAPI

        app = app_client.app
        assert isinstance(app, FastAPI)
        app.dependency_overrides[KoboProxyService] = lambda: mock_proxy

        try:
            response = app_client.put(
                "/api/kobo/test_token/v1/library/remote-id/state",
                json={"ReadingStates": []},
            )

            assert response.status_code == 200
            assert response.json() == {"updated": True}

            mock_proxy.proxy_request.assert_called_once()
        finally:
            app.dependency_overrides.pop(KoboProxyService, None)

    def test_update_reading_state_new_entry(
        self, app_client: TestClient, temp_db: str
    ) -> None:
        from sqlmodel import Session, create_engine, select

        engine = create_engine(temp_db, connect_args={"check_same_thread": False})

        book_id = str(uuid4())

        with Session(engine) as session:
            book = Book(
                id=UUID(book_id),
                title="Test Book",
                file_path="/tmp/test.epub",
                file_hash="hash",
                updated_at=datetime.now(UTC),
            )
            session.add(book)
            session.commit()

        payload = {
            "ReadingStates": [
                {
                    "EntitlementId": book_id,
                    "StatusInfo": {"Status": "Reading"},
                    "Statistics": {"SpentReadingMinutes": 10},
                    "CurrentBookmark": {
                        "ProgressPercent": 50,
                        "Location": {"Value": "loc"},
                    },
                }
            ]
        }

        response = app_client.put(
            f"/api/kobo/test_token/v1/library/{book_id}/state", json=payload
        )
        assert response.status_code == 200

        # Verify DB state
        with Session(engine) as session:
            state = session.exec(
                select(ReadingState).where(ReadingState.book_id == UUID(book_id))
            ).first()
            assert state is not None
            assert state.status == "Reading"
            assert state.spent_reading_minutes == 10
            assert state.progress_percent == 50
            assert state.location_value == "loc"
