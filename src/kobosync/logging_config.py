import logging
import sys
from typing import Any, cast

import structlog


def configure_logging(level: str = "INFO") -> None:
    """Configure structlog for JSON output."""
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.UnicodeDecoder(),
            structlog.processors.format_exc_info,
            structlog.processors.EventRenamer("message"),
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )


def get_logger(
    name: str | None = None, **initial_context: Any
) -> structlog.stdlib.BoundLogger:
    logger = structlog.get_logger(name)
    if initial_context:
        logger = logger.bind(**initial_context)
    return cast("structlog.stdlib.BoundLogger", logger)
