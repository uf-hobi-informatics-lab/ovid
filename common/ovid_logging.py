"""
OVID Logging Module

Provides centralized logging configuration for OVID pipelines.
Log files are stored in work_directory/docker_data/ovid_logs/ with the naming format:
    ovid_{timestamp}_{partner}.log

where timestamp uses the same format as the Spark master name (%Y%m%d-%H%M%S).
"""

import logging
import os
from datetime import datetime
from typing import Optional

# ANSI color codes
GREEN = '\033[92m'
RESET = '\033[0m'

# Global logger instance
_logger: Optional[logging.Logger] = None
_log_file_path: Optional[str] = None


class ColoredConsoleFormatter(logging.Formatter):
    """Custom formatter that adds green color to console output."""
    
    def format(self, record):
        # Format the message first
        message = super().format(record)
        # Wrap entire message in green color
        return f"{GREEN}{message}{RESET}"


def _get_log_directory() -> str:
    """
    Determine the appropriate log directory based on the runtime environment.
    
    Inside Docker containers (running from /app), use /docker_data/ovid_logs
    On the host, use work_directory/docker_data/ovid_logs
    
    Returns:
        Path to the log directory
    """
    # Check if running inside Docker container by checking if /docker_data exists
    # and we're running from /app
    if os.path.exists('/docker_data') and os.getcwd() == '/app':
        return '/docker_data/ovid_logs'
    # Host path
    return 'work_directory/docker_data/ovid_logs'


def setup_logging(
    timestamp: str,
    partner: str,
    log_level: int = logging.INFO,
    console_output: bool = True,
    base_dir: Optional[str] = None
) -> logging.Logger:
    """
    Initialize and configure the OVID logger.

    Args:
        timestamp: Timestamp string in format %Y%m%d-%H%M%S (same as Spark master name)
        partner: Partner code (e.g., 'avh', 'ufh', 'tmc')
        log_level: Logging level (default: logging.INFO)
        console_output: Whether to also output logs to console (default: True)
        base_dir: Base directory for log files (auto-detected if None)

    Returns:
        Configured logger instance
    """
    global _logger, _log_file_path

    # Auto-detect log directory if not specified
    if base_dir is None:
        base_dir = _get_log_directory()

    # Create logs directory if it doesn't exist
    os.makedirs(base_dir, exist_ok=True)

    # Create log filename: ovid_{timestamp}_{partner}.log
    # Replace dash with underscore in timestamp for consistency
    timestamp_clean = timestamp.replace('-', '_')
    log_filename = f"ovid_{timestamp_clean}_{partner}.log"
    _log_file_path = os.path.join(base_dir, log_filename)

    # Ensure log file exists and is writable by all (for Docker container access)
    if not os.path.exists(_log_file_path):
        # Create empty file
        open(_log_file_path, 'a').close()
    # Set world-writable permissions so container can write to it
    try:
        os.chmod(_log_file_path, 0o666)
    except OSError:
        pass  # Ignore if we can't change permissions (e.g., not owner)

    # Create or get logger
    _logger = logging.getLogger("ovid")
    _logger.setLevel(log_level)

    # Clear any existing handlers to avoid duplicate logs
    _logger.handlers.clear()

    # Create formatter for file (no colors)
    file_formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )

    # File handler - always add
    file_handler = logging.FileHandler(_log_file_path, mode='a', encoding='utf-8')
    file_handler.setLevel(log_level)
    file_handler.setFormatter(file_formatter)
    _logger.addHandler(file_handler)

    # Console handler - optional (with green color)
    if console_output:
        console_formatter = ColoredConsoleFormatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )
        console_handler = logging.StreamHandler()
        console_handler.setLevel(log_level)
        console_handler.setFormatter(console_formatter)
        _logger.addHandler(console_handler)

    _logger.info(f"OVID Logging initialized - Log file: {_log_file_path}")
    _logger.info(f"Partner: {partner} | Timestamp: {timestamp}")

    return _logger


def get_logger() -> logging.Logger:
    """
    Get the current OVID logger instance.

    Returns:
        Logger instance. If not initialized, returns a basic logger with WARNING level.
    """
    global _logger
    if _logger is None:
        # Return a basic logger if not initialized
        _logger = logging.getLogger("ovid")
        _logger.setLevel(logging.WARNING)
        if not _logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
                datefmt="%Y-%m-%d %H:%M:%S"
            ))
            _logger.addHandler(handler)
    return _logger


def get_log_file_path() -> Optional[str]:
    """
    Get the current log file path.

    Returns:
        Path to the current log file, or None if logging not initialized.
    """
    return _log_file_path


def log_job_start(job_name: str, **kwargs):
    """
    Log the start of a job with optional parameters.

    Args:
        job_name: Name of the job being started
        **kwargs: Additional parameters to log
    """
    logger = get_logger()
    logger.info("=" * 60)
    logger.info(f"STARTING JOB: {job_name}")
    for key, value in kwargs.items():
        logger.info(f"  {key}: {value}")
    logger.info("=" * 60)


def log_job_end(job_name: str, success: bool = True, duration: Optional[float] = None):
    """
    Log the end of a job.

    Args:
        job_name: Name of the job that completed
        success: Whether the job completed successfully
        duration: Optional duration in seconds
    """
    logger = get_logger()
    status = "COMPLETED" if success else "FAILED"
    logger.info("-" * 60)
    logger.info(f"JOB {status}: {job_name}")
    if duration is not None:
        logger.info(f"  Duration: {duration:.2f} seconds")
    logger.info("-" * 60)


def log_table_processing(table_name: str, action: str, row_count: Optional[int] = None):
    """
    Log table processing events.

    Args:
        table_name: Name of the table being processed
        action: Action being performed (e.g., 'format', 'map', 'upload')
        row_count: Optional number of rows processed
    """
    logger = get_logger()
    msg = f"Table [{table_name}] - {action}"
    if row_count is not None:
        msg += f" - Rows: {row_count:,}"
    logger.info(msg)


def log_error(message: str, exc_info: bool = False):
    """
    Log an error message.

    Args:
        message: Error message
        exc_info: Whether to include exception traceback
    """
    logger = get_logger()
    logger.error(message, exc_info=exc_info)


def log_warning(message: str):
    """
    Log a warning message.

    Args:
        message: Warning message
    """
    logger = get_logger()
    logger.warning(message)


def log_debug(message: str):
    """
    Log a debug message.

    Args:
        message: Debug message
    """
    logger = get_logger()
    logger.debug(message)
