"""Logging configuration helpers for the Polymarket anomaly tracker."""

from __future__ import annotations

import logging
from datetime import UTC, datetime
from typing import TextIO

LOGGER_NAME = "polymarket_anomaly_tracker"
HANDLER_NAME = "pmat_console"


class StructuredConsoleFormatter(logging.Formatter):
    """Format records as predictable key-value console output."""

    def format(self, record: logging.LogRecord) -> str:
        timestamp = datetime.fromtimestamp(
            record.created,
            tz=UTC,
        ).isoformat(timespec="seconds")
        message = record.getMessage().replace('"', '\\"')
        return (
            f'timestamp={timestamp} level={record.levelname} '
            f'module={record.name} message="{message}"'
        )


def configure_logging(
    level: str | int = "INFO",
    *,
    stream: TextIO | None = None,
) -> logging.Logger:
    """Configure package logging once and avoid duplicate handlers."""

    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(level)
    logger.propagate = False

    handler = _get_console_handler(logger)
    if handler is None:
        handler = logging.StreamHandler(stream)
        handler.name = HANDLER_NAME
        logger.addHandler(handler)
    elif stream is not None:
        handler.setStream(stream)

    handler.setLevel(level)
    handler.setFormatter(StructuredConsoleFormatter())

    return logger


def reset_logging() -> None:
    """Remove package handlers so tests can configure logging from a clean state."""

    logger = logging.getLogger(LOGGER_NAME)
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()
    logger.propagate = True
    logger.setLevel(logging.NOTSET)


def _get_console_handler(logger: logging.Logger) -> logging.StreamHandler[TextIO] | None:
    for handler in logger.handlers:
        if isinstance(handler, logging.StreamHandler) and handler.name == HANDLER_NAME:
            return handler

    return None
