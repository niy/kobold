from collections.abc import Generator
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest


@pytest.fixture(autouse=True)
def minimized_test_delays() -> Generator[None]:
    """
    Automatically patch all polling intervals and rate limits to minimal values.
    """
    with (
        patch("kobosync.worker.WORKER_POLL_INTERVAL", 0.01),
        patch("kobosync.worker.WORKER_ERROR_BACKOFF", 0.01),
        patch("kobosync.watcher.WATCH_DEBOUNCE_MS", 1),
        patch.dict("os.environ", {"KS_WATCH_POLL_DELAY_MS": "1"}),
        patch("kobosync.scheduler.RECONCILE_INTERVAL_MINUTES", 1),
        patch("kobosync.metadata.base.SCRAPER_MIN_DELAY", 0.0),
        patch("kobosync.metadata.base.SCRAPER_JITTER_MAX", 0.0),
    ):
        yield


@pytest.fixture(autouse=True)
def mock_amazon_provider() -> Generator[AsyncMock]:
    with patch("kobosync.metadata.manager.AmazonProvider") as MockProvider:
        mock = MockProvider.return_value
        mock.fetch_metadata = AsyncMock(return_value=None)
        yield mock


@pytest.fixture(autouse=True)
def mock_goodreads_provider() -> Generator[AsyncMock]:
    with patch("kobosync.metadata.manager.GoodreadsProvider") as MockProvider:
        mock = MockProvider.return_value
        mock.fetch_metadata = AsyncMock(return_value=None)
        yield mock


@pytest.fixture(autouse=True)
def mock_kepub_converter() -> Generator[AsyncMock]:
    with patch("kobosync.worker.KepubConverter") as MockConverterCls:
        mock = MockConverterCls.return_value

        async def mock_convert_impl(input_path: Path, output_path: Path) -> Path | None:
            if not input_path.exists():
                return None
            output_path.write_bytes(b"mock kepub content")
            return output_path

        mock.convert = AsyncMock(side_effect=mock_convert_impl)
        yield mock


@pytest.fixture(autouse=True)
def mock_proxy_service() -> Generator[AsyncMock]:
    from fastapi import Response

    from kobosync.api.proxy import KoboProxyService
    from kobosync.main import app

    mock_proxy = AsyncMock(spec=KoboProxyService)
    mock_proxy.fetch_kobo_sync = AsyncMock(return_value=(200, {}, []))
    mock_proxy.proxy_request = AsyncMock(
        return_value=Response(content="{}", status_code=200)
    )

    app.dependency_overrides[KoboProxyService] = lambda: mock_proxy
    yield mock_proxy
    app.dependency_overrides.pop(KoboProxyService, None)
