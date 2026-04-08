"""Logging setup. Console for CLI, JSON for server.

Usage:
    from app.util.log import log
    log.info("thing happened", key=value)

Call configure_logging() at startup to set mode.
Defaults to console (auto-detected by structlog).
"""

import structlog


def configure_logging(json: bool = False):
    if json:
        renderer = structlog.processors.JSONRenderer()
        timestamper = structlog.processors.TimeStamper(
            fmt='iso'
        )
    else:
        renderer = structlog.dev.ConsoleRenderer()
        timestamper = structlog.processors.TimeStamper(
            fmt='%H:%M:%S'
        )

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            timestamper,
            renderer,
        ],
    )


configure_logging()

log = structlog.get_logger()
