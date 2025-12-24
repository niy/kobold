from functools import lru_cache
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    Application configuration.

    All variables are prefixed with KS_ (e.g. KS_PORT, KS_WATCH_DIRS).
    """

    # Watcher Configuration
    WATCH_DIRS: str = "/books"
    WATCH_FORCE_POLLING: bool = False
    WATCH_POLL_DELAY_MS: int = (
        300  # Polling interval (only used when WATCH_FORCE_POLLING=True)
    )

    # Core Settings
    USER_TOKEN: str
    CONVERT_EPUB: bool = True
    DELETE_ORIGINAL_AFTER_CONVERSION: bool = False
    EMBED_METADATA: bool = False

    # Data Directory
    DATA_PATH: Path = Field(default=Path("./data"))

    # Amazon Scraper Configuration
    AMAZON_DOMAIN: str = "com"
    AMAZON_COOKIE: str | None = None

    # Logging
    LOG_LEVEL: str = "INFO"

    model_config = SettingsConfigDict(
        env_prefix="KS_",
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    @property
    def watch_dirs_list(self) -> list[Path]:
        return [Path(d.strip()) for d in self.WATCH_DIRS.split(",") if d.strip()]

    @property
    def db_url(self) -> str:
        return f"sqlite:///{self.DATA_PATH}/kobosync.db"

    @property
    def bin_path(self) -> Path:
        return Path("bin")


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
                f"Missing required environment variable(s): {', '.join(f'KS_{m}' for m in missing)}"
            ) from None
        raise
