import logging
import sys
from pathlib import Path
from typing import Optional


def setup_logger(
    log_file: Optional[Path] = None,
    log_level: int = logging.INFO,
    module_name: str = "cardify",
) -> logging.Logger:
    """
    Set up module logger with console and optional file handlers.

    Args:
        log_file: Optional path to log file
        log_level: Logging level (default: INFO)
        module_name: Name of the module for the logger

    Returns:
        Configured logger instance
    """
    # Create logger
    logger = logging.getLogger(module_name)
    logger.setLevel(log_level)

    # Prevent adding handlers multiple times
    if logger.hasHandlers():
        return logger

    # Create formatters with line numbers and function names
    console_formatter = logging.Formatter(
        fmt="%(levelname)s [%(filename)s:%(lineno)d] %(funcName)s: %(message)s"
    )
    file_formatter = logging.Formatter(
        fmt="%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d in %(funcName)s] - %(message)s"
    )

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)

    # File handler (if log_file specified)
    if log_file:
        # Ensure directory exists
        log_file.parent.mkdir(parents=True, exist_ok=True)

        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)

    return logger


logger = setup_logger()
