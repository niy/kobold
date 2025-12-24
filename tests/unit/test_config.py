import os
from unittest.mock import patch

import pytest

from kobosync.config import get_settings


class TestConfigValidation:
    @pytest.fixture(autouse=True)
    def ignore_env_file(self):
        """Ignore local .env file for all tests in this class."""
        from kobosync.config import Settings

        original_config = Settings.model_config.copy()
        Settings.model_config["env_file"] = None
        yield
        Settings.model_config = original_config

    def test_missing_required_token_exits(self) -> None:
        """Ensure app exits if KS_USER_TOKEN is missing."""
        # Clear cache to force reload
        get_settings.cache_clear()

        # Determine environment with missing token
        env = os.environ.copy()
        if "KS_USER_TOKEN" in env:
            del env["KS_USER_TOKEN"]

        with patch.dict(os.environ, env, clear=True):
            with pytest.raises(SystemExit) as exc:
                get_settings()

            assert "Missing required environment variable(s): KS_USER_TOKEN" in str(
                exc.value
            )

    def test_minimal_config_succeeds(self) -> None:
        """Ensure app starts with only required variables."""
        get_settings.cache_clear()

        env = {"KS_USER_TOKEN": "valid-token"}

        with patch.dict(os.environ, env, clear=True):
            settings = get_settings()
            assert settings.USER_TOKEN == "valid-token"
            # Verify defaults are set
            assert settings.LOG_LEVEL == "INFO"
            assert settings.WATCH_DIRS == "/books"

    def test_optional_vars_override_defaults(self) -> None:
        """Ensure optional variables override defaults."""
        get_settings.cache_clear()

        env = {
            "KS_USER_TOKEN": "valid-token",
            "KS_LOG_LEVEL": "DEBUG",
            "KS_CONVERT_EPUB": "false",
        }

        with patch.dict(os.environ, env, clear=True):
            settings = get_settings()
            assert settings.LOG_LEVEL == "DEBUG"
            assert settings.CONVERT_EPUB is False
