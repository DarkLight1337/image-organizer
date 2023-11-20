from __future__ import annotations

import logging
import sys

__all__ = ['get_logger', 'set_logger_level']

_logger: logging.Logger | None = None

default_handler = logging.StreamHandler(sys.stdout)
default_handler.setFormatter(logging.Formatter('[%(levelname)s] %(message)s'))

def get_logger() -> logging.Logger:
    """Gets the logger used in this module. This is a singleton instance."""
    global _logger

    if _logger is None:
        _logger = logging.getLogger('image_organizer')
        _logger.propagate = False
        _logger.addHandler(default_handler)

    return _logger

def set_logger_level(level: int):
    """Sets the logging level for this module."""
    get_logger().setLevel(level)
