"""
OVID Logging Module

Provides centralized logging configuration for OVID pipelines.
Log files are stored in work_directory/docker_data/ovid_logs/ with the naming format:
    ovid_{timestamp}_{partner}.log

where timestamp uses the same format as the Spark master name (%Y%m%d-%H%M%S).
"""

import fcntl
import logging
import os
import re
import subprocess
import sys
import threading
from datetime import datetime
from typing import Optional

# ANSI color codes
GREEN = '\033[92m'
RESET = '\033[0m'

# Global logger instance
_logger: Optional[logging.Logger] = None
_log_file_path: Optional[str] = None

# Terminal lock state — prevents concurrent instances from corrupting
# each other's live display when sharing the same terminal (tty).
_suppress_terminal_echo: bool = False
_terminal_lock_fd: Optional[int] = None


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
    console_output: bool = False,
    base_dir: Optional[str] = None
) -> logging.Logger:
    """
    Initialize and configure the OVID logger.

    Args:
        timestamp: Timestamp string in format %Y%m%d-%H%M%S (same as Spark master name)
        partner: Partner code (e.g., 'avh', 'ufh', 'tmc')
        log_level: Logging level (default: logging.INFO)
        console_output: Whether to also output logs to console (default: False)
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
        # Return a basic logger if not initialized - file-only, no console output
        _logger = logging.getLogger("ovid")
        _logger.setLevel(logging.WARNING)
        if not _logger.handlers:
            _logger.addHandler(logging.NullHandler())
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


def print_pipeline_summary(title: str, fields: list[tuple[str, str]]) -> None:
    """Print a formatted startup summary table for a pipeline.

    Displays a bordered table with the pipeline name and key-value
    configuration fields, using matrix-green styling consistent with
    the rest of the OVID terminal output.

    Args:
        title: Pipeline title (e.g., "OVID ETL Pipeline").
        fields: List of (label, value) tuples to display.
    """
    bold_green = '\033[1m\033[92m'
    white = '\033[97m'
    reset = '\033[0m'

    border = '=' * 70

    # Pad labels to the widest one so the colons align
    label_width = max(len(label) for label, _ in fields) if fields else 0

    lines: list[str] = []
    lines.append(f"{bold_green}{border}{reset}")
    lines.append(f"{bold_green}  {title}{reset}")
    lines.append(f"{bold_green}{border}{reset}")
    for label, value in fields:
        lines.append(
            f"{bold_green}  {label:<{label_width}}{reset}"
            f" {white}: {value}{reset}"
        )
    lines.append(f"{bold_green}{border}{reset}")

    print('\n'.join(lines), flush=True)

    # Log the plain-text version (no ANSI codes) to the OVID log file
    logger = get_logger()
    logger.info(f"--- {title} ---")
    for label, value in fields:
        logger.info(f"  {label}: {value}")


# ANSI escape code pattern for stripping color/cursor codes before writing to log file
_ANSI_ESCAPE_RE = re.compile(r'\x1b\[[0-9;]*[A-Za-z]|\x1b\[\?[0-9;]*[A-Za-z]|\r')


def _strip_ansi(text: str) -> str:
    """Remove ANSI escape codes and carriage returns from text."""
    return _ANSI_ESCAPE_RE.sub('', text)


# ─── Terminal Lock (TTY-scoped) ──────────────────────────────────────────────
#
# When two ovid.py instances share the same terminal (same tty device), their
# ANSI cursor-movement escape codes corrupt each other's live display.  The
# lock below ensures only the *first* instance on a given tty echoes output to
# the terminal; subsequent instances write to their log file only.
#
# Instances on *different* terminals (different tty devices) get independent
# lock files and are never affected.
# ─────────────────────────────────────────────────────────────────────────────


def acquire_terminal_lock(sync_dir: str) -> bool:
    """Try to acquire an exclusive lock scoped to the current terminal device.

    If stdout is not a tty (e.g. piped or redirected), no lock is acquired
    and the function returns True (no live display to protect).

    The lock is held for the lifetime of the process.  It is released
    automatically when the process exits, or explicitly via
    ``release_terminal_lock()``.

    Failure modes are distinguished:

    * ``BlockingIOError`` from ``flock()`` -- another live process holds the
      advisory lock on this tty. Suppress terminal echo (return False) so we
      do not corrupt the other instance's live display.
    * ``PermissionError`` from ``os.open()`` -- the lock file exists but is
      owned by another user (typically a stale file left behind by a prior
      ``sudo``/root invocation; the file persists across reboots and the
      stale-instance sweeper only touches ``work_directory/`` files, not
      ``.ovid_sync/``). No live process holds an advisory lock on this file
      because the file's permissions prevent us from even reaching ``flock``.
      Best-effort delete the stale file and retry once; on persistent failure
      log a warning and ALLOW terminal output (return True). The previous
      behaviour -- silently suppressing all terminal echo because of stale
      file permissions -- caused the user-visible "ran ovid command, saw
      nothing on terminal" symptom, which is far worse than the cosmetic
      collision risk we are guarding against.
    * Any other ``OSError`` -- log and allow output (same fail-open default).

    Args:
        sync_dir: Path to the ``.ovid_sync`` directory where lock files live.

    Returns:
        True if this instance owns the terminal (or stdout is not a tty,
        or the lock state was indeterminate due to filesystem issues).
        False only when another live process is verifiably holding the lock.
    """
    global _suppress_terminal_echo, _terminal_lock_fd
    logger = get_logger()

    # Nothing to protect if stdout is not a real terminal
    if not sys.stdout.isatty():
        return True

    try:
        tty_name = os.ttyname(sys.stdout.fileno())
    except OSError:
        # Cannot determine tty — play it safe, allow output
        return True

    # Sanitize tty name for use as a filename  (/dev/pts/3 → dev_pts_3)
    sanitized = tty_name.lstrip('/').replace('/', '_')
    lock_path = os.path.join(sync_dir, f'terminal_{sanitized}.lock')

    def _try_open_and_lock(path: str):
        """Open path and acquire exclusive non-blocking flock. Returns fd on
        success; raises the underlying OSError/BlockingIOError on failure."""
        # Force mode 0666 against a permissive umask so a permissive group
        # can clean up if the owning process dies without releasing.
        old_umask = os.umask(0)
        try:
            new_fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o666)
        finally:
            os.umask(old_umask)
        try:
            fcntl.flock(new_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except Exception:
            os.close(new_fd)
            raise
        return new_fd

    try:
        fd = _try_open_and_lock(lock_path)
        _terminal_lock_fd = fd
        _suppress_terminal_echo = False
        return True
    except BlockingIOError:
        # A live process truly holds the advisory lock on this tty.
        # Suppress echo to avoid corrupting its live display.
        _suppress_terminal_echo = True
        return False
    except PermissionError:
        # Stale file owned by another user (e.g. root from a prior sudo run).
        # No live process can be holding flock on a file we can't even open
        # for write -- the failure happened in os.open(), before flock(). Try
        # to remove the stale file and retry once; if that fails, fail open
        # (allow terminal output) since suppressing output silently is the
        # worse outcome for the user.
        try:
            os.unlink(lock_path)
            logger.info(
                f"Removed stale root-owned terminal lock file: {lock_path}"
            )
            try:
                fd = _try_open_and_lock(lock_path)
                _terminal_lock_fd = fd
                _suppress_terminal_echo = False
                return True
            except Exception as retry_exc:
                logger.warning(
                    f"Could not acquire terminal lock after removing stale file "
                    f"{lock_path}: {retry_exc}. Allowing terminal output anyway.",
                )
                _suppress_terminal_echo = False
                return True
        except OSError as unlink_exc:
            logger.warning(
                f"Stale terminal lock file {lock_path} is not removable "
                f"({unlink_exc}); allowing terminal output anyway. "
                f"Run `sudo rm {lock_path}` to clean up.",
            )
            _suppress_terminal_echo = False
            return True
    except OSError as exc:
        # Any other filesystem error -- fail open with a warning so the user
        # at least sees their command's output.
        logger.warning(
            f"Could not acquire terminal lock at {lock_path}: {exc}. "
            f"Allowing terminal output anyway.",
        )
        _suppress_terminal_echo = False
        return True


def release_terminal_lock() -> None:
    """Release the terminal lock acquired by ``acquire_terminal_lock()``.

    Safe to call even if no lock was acquired (no-op in that case).
    """
    global _terminal_lock_fd, _suppress_terminal_echo

    if _terminal_lock_fd is not None:
        try:
            fcntl.flock(_terminal_lock_fd, fcntl.LOCK_UN)
            os.close(_terminal_lock_fd)
        except OSError:
            pass
        _terminal_lock_fd = None
    _suppress_terminal_echo = False


def _stream_to_log(pipe, log_file_path: str, echo_stream):
    """
    Read lines from a subprocess pipe and write them to both the console
    and the log file.  Runs in a background thread.

    Args:
        pipe: A readable pipe (stdout or stderr from subprocess.Popen).
        log_file_path: Path to the OVID log file.
        echo_stream: sys.stdout or sys.stderr – where to echo live output.
    """
    try:
        with open(log_file_path, 'a', encoding='utf-8') as log_fh:
            for raw_line in iter(pipe.readline, ''):
                # Echo to console with original ANSI codes so the display works.
                # Suppressed when another OVID instance owns this terminal.
                if not _suppress_terminal_echo:
                    echo_stream.write(raw_line)
                    echo_stream.flush()

                # Strip ANSI codes and write clean text to the log file
                clean = _strip_ansi(raw_line).rstrip('\n')
                if clean.strip():  # skip blank lines from cursor movements
                    log_fh.write(clean + '\n')
                    log_fh.flush()
    except (ValueError, OSError):
        # Pipe closed – nothing to do
        pass


def run_and_log_subprocess(command, env=None, shell=False, label: str = "subprocess"):
    """
    Run a subprocess while teeing its stdout and stderr to the OVID log file
    in real time.  Output is also echoed to the console so existing terminal
    formatting (ANSI colours, timer updates) continues to work.

    Args:
        command: Command list or string to execute.
        env: Optional environment dict for the subprocess.
        shell: Whether to run with shell=True (default False).
        label: A human-readable label for log messages.

    Returns:
        The subprocess.Popen return code.
    """
    logger = get_logger()
    log_path = get_log_file_path()

    # Fall back to plain subprocess.run if logging isn't initialised
    if log_path is None:
        logger.warning(f"[{label}] Logging not initialised – running without log capture")
        result = subprocess.run(command, env=env, shell=shell)
        return result.returncode

    logger.info(f"[{label}] Starting subprocess – capturing output to log")

    # start_new_session=True puts the subprocess in its own session/process group
    # so signals delivered to the parent shell (e.g. SIGINT from a non-tty
    # parent harness like opencode/ansible/CI when its bash invocation ends)
    # do NOT propagate into the docker-exec'd pipeline running inside the
    # cluster master container. The host-side ovid.py still handles SIGINT
    # via its own _signal_handler() which calls _shutdown_cluster(); this
    # change only prevents accidental, parent-driven termination of the
    # in-container python process before the live-display streamer thread
    # has had a chance to drain stdout to the log file.
    proc = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        env=env,
        shell=shell,
        bufsize=1,          # line-buffered
        start_new_session=True,
    )

    # Tee stdout and stderr in parallel threads
    stdout_thread = threading.Thread(
        target=_stream_to_log,
        args=(proc.stdout, log_path, sys.stdout),
        daemon=True,
    )
    stderr_thread = threading.Thread(
        target=_stream_to_log,
        args=(proc.stderr, log_path, sys.stderr),
        daemon=True,
    )
    stdout_thread.start()
    stderr_thread.start()

    proc.wait()
    stdout_thread.join()
    stderr_thread.join()

    if proc.returncode != 0:
        logger.warning(f"[{label}] Subprocess exited with code {proc.returncode}")
    else:
        logger.info(f"[{label}] Subprocess completed successfully")

    return proc.returncode
