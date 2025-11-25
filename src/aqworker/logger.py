"""
Basic logger configuration for aqworker.
"""
import logging
import os
import sys
from typing import Optional

from aqworker.constants import LOGGER_NAME


def setup_logger(
    name: str = LOGGER_NAME,
    level: str = "INFO",
    format_string: Optional[str] = None,
) -> logging.Logger:
    """
    Setup and configure a logger.

    Args:
        name: Logger name (default: "aqworker")
        level: Logging level (default: "INFO")
        format_string: Custom format string for log messages

    Returns:
        Configured logger instance
    """
    _logger = logging.getLogger(name)

    # Avoid adding multiple handlers if logger is already configured
    if _logger.handlers:
        return _logger

    # Set logging level
    log_level = getattr(logging, level.upper(), logging.INFO)
    _logger.setLevel(log_level)

    # Create console handlers
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(log_level)

    # Create formatter
    if format_string is None:
        format_string = (
            "%(asctime)s - %(name)s - %(levelname)s - "
            "%(filename)s:%(lineno)d - %(message)s"
        )

    formatter = logging.Formatter(format_string, datefmt="%Y-%m-%d %H:%M:%S")
    console_handler.setFormatter(formatter)

    # Add handlers to logger
    _logger.addHandler(console_handler)

    # Prevent propagation to root logger
    _logger.propagate = False

    return _logger


# Default logger instance
logger = setup_logger(
    level=os.environ.get("AQWORKER_LOG_LEVEL", "INFO").upper(),
)
