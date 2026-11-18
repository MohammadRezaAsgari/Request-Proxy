import logging

from gateway.config import settings

LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"


def configure_logging() -> None:
    level = settings.log_level.upper()

    if level not in logging.getLevelNamesMapping():
        raise ValueError(f"Invalid log level: {settings.log_level!r}")

    logging.basicConfig(
        level=level,
        format=LOG_FORMAT,
        force=True,
    )
