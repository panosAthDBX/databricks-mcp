import logging
import sys

import structlog

from .config import settings


def setup_logging():
    """Configure structured logging using structlog."""

    # Define processors for structlog
    shared_processors = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.UnicodeDecoder(),
    ]

    # Configure standard logging to work with structlog
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=settings.numeric_log_level,
    )

    # Configure structlog
    structlog.configure(
        processors=shared_processors + [
            # Use JSONRenderer for structured output
            structlog.processors.JSONRenderer(),
        ],
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # Example of getting a logger
    # log = structlog.get_logger("databricks_mcp")
    # log.info("Logging setup complete", log_level=settings.log_level)
