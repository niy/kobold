from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict

from .constants import PROJECT_ROOT


class Settings(BaseSettings):
    """
    Application configuration.

    All variables are prefixed with KB_ (e.g. KB_PORT, KB_WATCH_DIRS).
    """

    # Watcher Configuration
    WATCH_DIRS: str = "/books"
    WATCH_FORCE_POLLING: bool = False
    WATCH_POLL_DELAY_MS: int = (
        300  # Polling interval (only used when WATCH_FORCE_POLLING=True)
    )

    # Interval between polling for new jobs
    WORKER_POLL_INTERVAL: float = 300.0

    # Core Settings
    USER_TOKEN: str
    CONVERT_EPUB: bool = True
    DELETE_ORIGINAL_AFTER_CONVERSION: bool = False
    EMBED_METADATA: bool = False
    FETCH_EXTERNAL_METADATA: bool = True

    # Library Organization
    ORGANIZE_LIBRARY: bool = False
    ORGANIZE_TEMPLATE: str = "{author}/{title}"

    # Data Directory
    DATA_PATH: Path = Field(default_factory=lambda: PROJECT_ROOT / "data")

    # Amazon Scraper Configuration
    AMAZON_DOMAIN: str = "com"
    AMAZON_COOKIE: str | None = None

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_prefix="KB_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def watch_dirs_list(self) -> list[Path]:
        return [Path(d.strip()) for d in self.WATCH_DIRS.split(",") if d.strip()]

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.DATA_PATH}/kobold.db"

    @property
    def tools_path(self) -> Path:
        return PROJECT_ROOT / ".tools"


@lru_cache
def get_settings() -> Settings:
    """Load settings from environment, failing fast if required values are missing."""
    from pydantic import ValidationError

    try:
        return Settings.model_validate({})
    except ValidationError as e:
        missing = [err["loc"][0] for err in e.errors() if err["type"] == "missing"]
        if missing:
            raise SystemExit(
                f"Missing required environment variable(s): {', '.join(f'KB_{m}' for m in missing)}"
            ) from None
        raise
