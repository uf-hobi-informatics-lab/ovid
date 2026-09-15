"""
OVID Scheduler — host-side parallel orchestrator.

Architecture (since 2026-06-12, replaces the in-container legacy scheduler):

  - Runs as a host-side Python process (typically managed by systemd via
    ovid-scheduler.service). NO long-lived master container is owned by
    the scheduler -- the scheduler is pure orchestration, no Spark.
  - Maintains N parallel "slots" (default N = SCHEDULER_PARALLEL_SLOTS,
    typically 2). Each slot can run one batch end-to-end.
  - Each batch is launched via `subprocess.Popen` of a fresh
    `ovid etl ...` command. That subprocess uses the existing 2026-03-28
    instance isolation in ovid.py to spin up its own master + workers,
    run the ETL pipeline, then tear down.
  - After ETL finishes, the slot launches `ovid tools -j raw_upload ...`
    against the same partner/folder (mirroring the legacy scheduler's
    post-ETL raw_upload stage). The raw_upload's success/failure is
    recorded but never flips the batch's data_batch.status.
  - Each slot redirects its subprocess stdout/stderr (raw ANSI bytes,
    preserving the live progress display) to a per-batch log file:
        {SCHEDULER_SLOT_LOG_DIR}/systemd-scheduler-batch-slot<N>-<batch_id>-<ts>.log
    Each batch gets its own permanent file (never reused, never
    truncated), so `ovid-watch` shows one stable page per batch. The
    slot index is embedded in the filename so the viewer can render
    ``slot N`` page labels without any IPC back to the daemon (since
    2026-06-25). Files written by pre-2026-06-25 daemons (lacking the
    ``slot<N>-`` segment) and pre-2026-06-16 daemons (the per-slot
    ``systemd-scheduler-slot-<N>.log`` files) are still recognised by
    ovid-watch via legacy patterns for graceful migration.
  - The drain loop runs a CONTINUOUS dispatcher (since 2026-06-25):
    every iteration fetches the current set of NEW batches, sorts them
    FCFS by received_time, and dispatches to any free slot immediately.
    The loop then waits for ANY slot future to complete (with a short
    inner poll, SCHEDULER_DRAIN_RECHECK_SECONDS, default 30s) before
    re-fetching. Long-running batches no longer starve incoming ones:
    the moment a slot frees, the next FCFS batch dispatches.  SIGUSR1
    (sent by `ovid-watch`'s 'r' key) short-circuits both the inner
    poll wait and the outer empty-queue sleep so an operator can force
    a rescan at any time.
  - The Snowflake state machine (OVID.SCHEDULER.data_batch) is the
    single source of truth for batch status; concurrent slots are safe
    because each batch_id is uniquely written by exactly one slot.

Legacy in-container scheduler (pipelines/scheduler_legacy.py) is preserved
for emergency rollback; flip SCHEDULER_MODE = 'legacy' in common/settings.py
and restart the daemon to revert.
"""

import csv
import datetime
import importlib
import os
import re
import shlex
import signal
import socket
import subprocess
import sys
import threading
import time
from concurrent.futures import (
    ThreadPoolExecutor,
    Future,
    wait,
    FIRST_COMPLETED,
)
from typing import Any, Optional

import pytz

# Add common directory to path for imports.  Unlike the legacy scheduler
# (which ran inside the master container with /app/common available),
# this orchestrator runs on the host so sys.path is computed relative
# to this file's location.
#
# IMPORTANT: this orchestrator is host-side and MUST NOT import
# common/commonFunctions.py.  That module pulls in PySpark + the
# Crypto.Cipher.XOR shim (which only exists inside the Docker image),
# so importing it on the host fails at module-load time.  Snowflake
# state-machine queries and SF auth are inlined below using
# snowflake.connector + cryptography directly.
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_REPO_ROOT = os.path.dirname(_THIS_DIR)
sys.path.insert(0, os.path.join(_REPO_ROOT, 'common'))
sys.path.insert(0, _REPO_ROOT)

import snowflake.connector  # noqa: E402
from cryptography.hazmat.backends import default_backend  # noqa: E402
from cryptography.hazmat.primitives import serialization  # noqa: E402

import ovid_secrets  # noqa: E402
from settings import (  # noqa: E402
    SCHEDULER_DB_NAME,
    SCHEDULER_DB_SCHEMA,
    SCHEDULER_DB_TYPE,
    SCHEDULER_DB_SERVER,
    SCHEDULER_PARALLEL_SLOTS,
    SCHEDULER_WORKERS_PER_SLOT,
    SCHEDULER_SLOT_LOG_DIR,
    SCHEDULER_POLL_INTERVAL,
    SCHEDULER_POLL_INTERVAL_IN_MINUTES,
    SCHEDULER_DRAIN_RECHECK_SECONDS,
    SCHEDULER_RAW_UPLOAD_ENABLED,
    SCHEDULER_EMAIL_ENABLED,
    SCHEDULER_EMAIL_RECIPIENTS,
    SCHEDULER_EMAIL_FROM,
    SCHEDULER_EMAIL_SUBJECT_PREFIX,
    SCHEDULER_SMTP_HOST,
    SCHEDULER_SMTP_PORT,
    SCHEDULER_SMTP_TIMEOUT_SECONDS,
    SCHEDULER_EMAIL_MAX_ERROR_LINES,
    MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES,
    SCHEDULER_READY_MARKER_ENABLED,
    SCHEDULER_READY_MARKER_FILENAMES,
    PCORNET_PRIMARY_KEYS,
    CREATE_BATCH_TABLE_DLL,
    CREATE_BATCH_TABLE_FILE_DLL,
)
from email_notifier import BatchNotifier  # noqa: E402
from ovid_logging import (  # noqa: E402
    setup_logging,
    get_logger,
    get_log_file_path,
)


# ─── Module-level state ───────────────────────────────────────────────────

# Logger initialised by run_orchestrator() before the main loop starts.
_logger = None


class _WakeRequested(Exception):
    """Marker exception raised by the SIGUSR1 handler ONLY while the main
    thread is parked in ``time.sleep()``.

    Background:
      Python 3.5+ implements PEP 475, which makes ``time.sleep()`` (and
      most other syscalls) automatically resume after a signal handler
      runs IF the handler returns normally.  This means a handler that
      just logs and returns CANNOT wake the orchestrator from its
      ``time.sleep(SCHEDULER_POLL_INTERVAL)`` -- the kernel returns
      EINTR, Python catches it, runs our handler, then re-enters
      ``nanosleep(2)`` for the remaining duration.

      The standard idiom for "I want a signal-interruptible sleep" in
      Python 3.5+ is:
          1. The handler raises an exception.
          2. The caller wraps ``time.sleep()`` in ``try/except ThatExc``.

      But the handler MUST NOT raise unconditionally: if SIGUSR1 arrives
      while the main thread is inside drain_cycle() (e.g. mid Snowflake
      auth handshake, mid ``os.listdir``, mid network I/O), raising
      ``_WakeRequested`` corrupts that operation -- the connector's
      try/except clauses catch it as a generic Exception and re-wrap it
      as auth-failure errors, killing the entire scan.  See the 2026-06-12
      ``Failed to load private key`` cascade in the Change History.

    The fix is a two-state mechanism:
      - ``_sleep_active`` (set to True only while ``time.sleep`` is on the
        main-thread call stack): the handler raises ``_WakeRequested``
        only when this is True.  The exception unwinds through
        ``time.sleep()`` and into the inner ``except _WakeRequested:``.
      - ``_wake_pending`` (a ``threading.Event``): set unconditionally by
        the handler.  The main loop checks it at the top of every
        iteration and SKIPS the sleep if set.  This handles the case
        where SIGUSR1 arrived during drain_cycle() -- no exception is
        ever raised, no Snowflake query is corrupted, but the next
        sleep is bypassed so the rescan happens immediately.

      Together they guarantee:
        (a) SIGUSR1 during sleep -> sleep returns immediately.
        (b) SIGUSR1 during drain -> drain finishes cleanly, next sleep
            is skipped.
        (c) Multiple rapid presses collapse to a single wake (Event is
            edge-triggered, not counted).

    Not exposed in the public API; underscore-prefixed name reflects that.
    """
    pass


# Wake-state flags consulted by ``_handle_wake_signal`` and the main loop.
# Both are set/cleared on the main thread only (signal handlers always
# run on the main thread in Python), so no lock is required.
#
# - ``_sleep_active`` is a single-element list (mutable across the
#   handler closure; bool would need a global declaration in every site).
#   True ONLY between the start of ``time.sleep`` and its return.
# - ``_wake_pending`` is a threading.Event used purely as an edge-triggered
#   "skip the next sleep" signal.  Cleared at the top of each loop iteration.
_sleep_active: list[bool] = [False]
_wake_pending: threading.Event = threading.Event()

# Path to the host-side `ovid` wrapper. The orchestrator spawns ETL and
# raw_upload subprocesses through this wrapper so each batch reuses all
# of ovid.py's instance-isolation, terminal-lock, and signal-handling
# code paths -- the same ones a human operator would invoke.
_OVID_WRAPPER_DEFAULT = '/usr/local/bin/ovid'


# ─── Host-vs-container partner-data path translation ─────────────────────
#
# Background:
#   In legacy mode the scheduler ran INSIDE the master Docker container,
#   where /data was a bind mount of the host's /data/inboxarchive. Every
#   data_batch row inserted then -- and every -f argv passed to the
#   in-container ETL/raw_upload subprocess -- uses the container-relative
#   form '/data/<PARTNER>/<FOLDER>'.
#
#   The 2026-06-12 orchestrator-mode rewrite moved the scheduler ONTO the
#   host. The host filesystem has /data/inboxarchive, NOT /data/<PARTNER>.
#   So host-side filesystem operations (os.listdir, os.path.exists,
#   os.path.getctime) must remap '/data/<rest>' to
#   '/data/inboxarchive/<rest>' before touching the kernel.
#
#   Paths handed to subprocesses (the -f flag for `ovid etl` / `ovid tools`,
#   the data_batch.path column, the ovid_input_data_folder_name field of
#   the job dict) are PRESERVED in the container-relative form, so the
#   in-container ETL pipeline -- whose /data IS /data/inboxarchive via
#   the docker-compose bind -- continues to resolve them correctly.
#
#   This helper is the one place where the translation happens. Call it
#   immediately before any host-side os.* call against a partner-data path.
HOST_INPUT_DIR = '/data/inboxarchive'


def _host_path_for_partner_data(container_path: str) -> str:
    """Translate a container-relative partner-data path to its host form.

    Args:
        container_path: An absolute path the in-container ETL would see,
            e.g. '/data/EMY/20260610' or '/data/EMY/20260610/file.csv'.

    Returns:
        The corresponding path on the host filesystem, e.g.
        '/data/inboxarchive/EMY/20260610'. Inputs that don't begin with
        '/data/' are returned unchanged so this helper is safe to call on
        arbitrary path strings (operator-edited DB rows, future code paths
        that already pass host paths, etc.).
    """
    prefix = '/data/'
    if container_path.startswith(prefix):
        return HOST_INPUT_DIR + '/' + container_path[len(prefix):]
    return container_path


# Regex matching the timestamp prefix emitted by ovid_logging's file handler:
#   "2026-04-29 12:34:56 | ERROR    | ovid | ..."
_LOG_LINE_RE = re.compile(
    r"^(?P<ts>\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})\s*\|\s*"
    r"(?P<level>[A-Z]+)\s*\|.*$"
)


# Slot tracking. _active_slots maps slot index (1..N) to the partner_folder
# string currently running on that slot, or None if free. Protected by
# _slot_lock.  Used by submit_to_slot() to find a free slot, and by
# `ovid-watch` (indirectly via the per-slot log file paths) so the operator
# can see what's running where.
_slot_lock = threading.Lock()
_active_slots: dict[int, Optional[str]] = {}

# Cascade-stop infrastructure (since 2026-06-16).
#
# Pre-2026-06-16 the orchestrator launched slot subprocesses with
# start_new_session=True specifically so a SIGINT into the orchestrator
# (e.g. systemctl stop) would NOT cascade — slot batches would finish
# gracefully. The user observed this was surprising operationally:
# `sudo systemctl stop ovid-scheduler` returned with the daemon
# "deactivating" but in-flight ETL/raw_upload subprocesses kept running
# silently in their docker compose projects, with no way for the
# operator to know the actual upload was still touching Snowflake.
#
# The new contract: `systemctl stop` is AUTHORITATIVE.  When the
# orchestrator's main loop detects shutdown (KeyboardInterrupt from
# the systemd KillSignal=SIGINT, or SIGTERM directly), it iterates
# every entry in `_active_slot_procs` and calls
# `os.killpg(proc.pid, signal.SIGTERM)` to dispatch SIGTERM to the
# entire process group of each running slot subprocess (the slot
# subprocess is the session leader thanks to start_new_session=True,
# so its pgid == its pid, and the group includes its docker-exec /
# bash / spark-submit / java / python descendants).  This brings down
# every cluster the daemon owns within seconds.
#
# Also: when `_shutdown_requested` is set, run_batch() skips the
# completion email entirely — there's no value in spamming the
# recipients with "ERROR" rows for batches that the operator
# deliberately killed.  Their data_batch rows stay PROCESSING and
# get reset to ERROR by the next daemon's startup recovery sweep.
_active_slot_procs: dict[int, "subprocess.Popen"] = {}
_active_slot_procs_lock = threading.Lock()
_shutdown_requested = threading.Event()


# ─── Snowflake connection & query helpers (host-side, no commonFunctions) ─


def _get_sf_connection(db_server: Optional[str] = None):
    """Open a Snowflake connection using RSA key-pair auth.

    Mirrors common.commonFunctions.CommonFuncitons.get_sf_connection but
    runs on the host (uses ovid_secrets.CREDENTIALS_PATH instead of the
    container's /ovid_credentials/ mount). All SF state-machine queries
    issued by this orchestrator go through this function.

    Args:
        db_server: Snowflake account locator. Defaults to SCHEDULER_DB_SERVER.

    Returns:
        An open snowflake.connector.connection.SnowflakeConnection.
        Caller is responsible for closing.
    """
    server = db_server or SCHEDULER_DB_SERVER

    creds_dir = ovid_secrets.CREDENTIALS_PATH
    passphrase_path = os.path.join(
        creds_dir, ovid_secrets.SF_PRIVATE_KEY_PASSPHRASE_FILE_NAME
    )
    key_path = os.path.join(creds_dir, ovid_secrets.SF_PRIVATE_KEY_FILE_NAME)

    with open(passphrase_path, "r") as fh:
        passphrase = fh.readline().strip()
    with open(key_path, "rb") as fh:
        private_key = serialization.load_pem_private_key(
            fh.read(),
            password=passphrase.encode() if passphrase else None,
            backend=default_backend(),
        )
    private_key_bytes = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )

    return snowflake.connector.connect(
        user=ovid_secrets.SF_DB_USER,
        private_key=private_key_bytes,
        account=server,
    )


def _execute_query(query: str) -> Optional[list]:
    """Execute a query against the scheduler DB and return all rows.

    Returns None on error (logged via _logger).  All scheduler-side
    state-machine queries route through here so SF connection lifecycle
    is uniform.
    """
    conn = None
    cursor = None
    try:
        conn = _get_sf_connection(SCHEDULER_DB_SERVER)
        cursor = conn.cursor()
        result = cursor.execute(query).fetchall()
        return result
    except Exception:
        if _logger is not None:
            _logger.error(
                f"Snowflake query failed: {query!r}", exc_info=True
            )
        return None
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _check_scheduler_db_exists() -> bool:
    """Return True if the OVID scheduler database exists in Snowflake."""
    conn = None
    cursor = None
    try:
        conn = _get_sf_connection(SCHEDULER_DB_SERVER)
        cursor = conn.cursor()
        cursor.execute(
            f"SHOW DATABASES LIKE '{SCHEDULER_DB_NAME.upper()}'"
        )
        result = cursor.fetchone()
        return result is not None
    except Exception:
        if _logger is not None:
            _logger.error("Could not check scheduler DB existence", exc_info=True)
        return False
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _create_scheduler_db() -> None:
    """Create OVID database / SCHEDULER schema / data_batch tables."""
    conn = None
    cursor = None
    try:
        conn = _get_sf_connection(SCHEDULER_DB_SERVER)
        cursor = conn.cursor()
        cursor.execute(f"CREATE DATABASE {SCHEDULER_DB_NAME}")
        cursor.execute(f"CREATE SCHEMA {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}")
        cursor.execute(CREATE_BATCH_TABLE_DLL)
        cursor.execute(CREATE_BATCH_TABLE_FILE_DLL)
    except Exception:
        if _logger is not None:
            _logger.error("Could not create scheduler DB", exc_info=True)
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _now_eastern_str() -> str:
    """Return current US/Eastern time formatted as 'YYYY-MM-DD HH:MM:SS'.

    Replaces commonFunctions.CommonFuncitons.get_current_time(); the legacy
    scheduler used _now_eastern_str() to populate processing_start /
    processing_end. Inlined here so the orchestrator stays standalone.
    """
    eastern = pytz.timezone("US/Eastern")
    return datetime.datetime.now(eastern).strftime("%Y-%m-%d %H:%M:%S")


# ─── Snowflake state-machine queries (unchanged from legacy) ─────────────


def batch_exist_in_scheduler_db(batch_id: str) -> bool:
    """Check whether a batch row exists in OVID.SCHEDULER.data_batch."""
    query = (
        f"SELECT COUNT(*) FROM {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch "
        f"WHERE batch_id = '{batch_id}'"
    )
    result = _execute_query(query)
    count = result[0][0] if result and len(result) > 0 else None
    return count != 0


def add_new_batch_to_db(
    batch_partner_name: str,
    batch_folder: str,
    original_partner_folder: Optional[str] = None,
) -> None:
    """Insert a new batch row with status='NEW' and received_time."""
    batch_id = f"{batch_partner_name}_{batch_folder}"
    folder_for_path = original_partner_folder if original_partner_folder else batch_partner_name
    # Container-relative path (stored verbatim in the DB so legacy rows
    # and -f argv values stay aligned). The host-side ctime lookup below
    # remaps to the actual filesystem location.
    batch_path = f"/data/{folder_for_path}/{batch_folder}"

    batch_received_time_float = os.path.getctime(
        _host_path_for_partner_data(batch_path)
    )
    utc_dt = datetime.datetime.utcfromtimestamp(
        batch_received_time_float
    ).replace(tzinfo=pytz.utc)
    eastern = pytz.timezone("US/Eastern")
    batch_received_time_est = utc_dt.astimezone(eastern)

    query = f"""
        INSERT INTO {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch
        (batch_id, path, received_time, status)
        VALUES('{batch_id}', '{batch_path}', '{batch_received_time_est}', 'NEW');
    """
    _execute_query(query)


def acknowledge_ready_signal(batch_id: str) -> None:
    """Acknowledge an event-driven ready-marker by advancing NEW -> READY.

    Called the first time a ready-marker file is detected for a batch that
    is still in the ``NEW`` state. Records the acknowledgement by stamping
    ``signal_received_time`` and transitioning the row to ``READY``. The
    update is guarded by ``status = 'NEW'`` so it is idempotent: a marker
    that lingers after the batch has already advanced (READY / PROCESSING /
    PROCESSED / ERROR) never re-triggers or rewinds the state machine.

    Args:
        batch_id: The ``{partner}_{folder}`` batch identifier.
    """
    query = f"""
        UPDATE {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch
        SET status = 'READY', signal_received_time = CURRENT_TIMESTAMP()
        WHERE batch_id = '{batch_id}' AND status = 'NEW';
    """
    try:
        _execute_query(query)
        _logger.info(
            f"Ready signal acknowledged for batch {batch_id}: marker file "
            f"detected; transitioned NEW -> READY (signal_received_time set)"
        )
    except Exception:
        _logger.error(
            f"Failed to acknowledge ready signal for batch {batch_id}; "
            f"the batch will still be dispatched but the READY transition "
            f"was not recorded",
            exc_info=True,
        )


def batch_ready_to_process(batch_id: str) -> bool:
    """Return True if the batch is eligible to dispatch.

    A batch is eligible when its status is still pending (``NEW`` or the
    acknowledged ``READY`` state) AND either:

      1. An event-driven ready-marker file is present in the batch folder
         (the archive process signalled completion — dispatch immediately);
         or
      2. The fallback quiet-period elapsed: the folder's ctime has not
         changed for more than
         ``MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES`` minutes (for
         partners/archives that do not yet emit a marker).

    When a marker is detected for a batch still in ``NEW``, the batch is
    acknowledged (``NEW -> READY`` + ``signal_received_time``) before this
    returns True, so the transition is observable in Snowflake and
    ovid-watch.

    Args:
        batch_id: The ``{partner}_{folder}`` batch identifier.

    Returns:
        True if the batch should be dispatched now; False otherwise.
    """
    query = (
        f"SELECT status, path FROM {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch "
        f"WHERE batch_id = '{batch_id}'"
    )
    result = _execute_query(query)
    status = result[0][0] if result and len(result) > 0 else None
    batch_path = result[0][1] if result and len(result) > 0 else None

    # Only NEW or already-acknowledged READY batches are dispatch candidates.
    if status not in ('NEW', 'READY'):
        return False

    # The DB stores container-relative paths (legacy convention from the
    # in-container scheduler). Translate to host before any os.* call --
    # see HOST_INPUT_DIR docs above for rationale.
    host_batch_path = (
        _host_path_for_partner_data(batch_path) if batch_path else None
    )

    if not host_batch_path or not os.path.exists(host_batch_path):
        return False

    # Event-driven fast path: an explicit ready-marker means the archive
    # finished writing, so skip the quiet-period entirely.
    if _folder_has_ready_marker(host_batch_path):
        if status == 'NEW':
            acknowledge_ready_signal(batch_id)
        return True

    # Fallback: no marker — fall back to the ctime quiet-period heuristic.
    batch_last_modified_time_float = os.path.getctime(host_batch_path)
    batch_last_modified_time = datetime.datetime.fromtimestamp(
        batch_last_modified_time_float
    )
    now = datetime.datetime.utcnow()
    time_diff_in_minutes = (now - batch_last_modified_time).total_seconds() / 60

    return time_diff_in_minutes > MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES


def update_batch_field(
    ovid_partner: str, ovid_folder: str, field_name: str, value: str
) -> None:
    """Update a single column on the data_batch row for a batch_id."""
    batch_id = f"{ovid_partner}_{ovid_folder}"
    query = f"""
        UPDATE {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.DATA_BATCH
        SET {field_name} = '{value}'
        WHERE BATCH_ID ILIKE '{batch_id}';
    """
    _execute_query(query)


def create_ovid_db_if_not_found() -> None:
    """Ensure the OVID.SCHEDULER schema and data_batch table exist."""
    if not _check_scheduler_db_exists():
        _create_scheduler_db()


def reset_processing_to_error_on_startup() -> None:
    """Reset any orphaned PROCESSING rows back to ERROR.

    Called once at orchestrator startup.  PROCESSING rows imply that an
    earlier orchestrator (or legacy scheduler) was killed mid-batch and
    has no chance of resuming — leaving them as PROCESSING would block
    the affected batches indefinitely (FCFS only picks up NEW rows).
    Mark them ERROR so the operator can manually re-trigger by setting
    them back to NEW after verifying the on-disk state is sane.
    """
    query = f"""
        UPDATE {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.DATA_BATCH
        SET status = 'ERROR'
        WHERE status = 'PROCESSING';
    """
    try:
        _execute_query(query)
        _logger.info(
            "Startup recovery: any orphaned PROCESSING rows reset to ERROR. "
            "Operator should manually inspect and re-trigger by setting "
            "status='NEW' if appropriate."
        )
    except Exception:
        _logger.error(
            "Startup recovery: failed to reset PROCESSING rows; continuing",
            exc_info=True,
        )


# ─── FCFS ordering helpers ───────────────────────────────────────────────


def get_batch_received_time(batch_id: str) -> Optional[datetime.datetime]:
    """Fetch received_time for a batch (used as FCFS sort key)."""
    query = (
        f"SELECT received_time FROM {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch "
        f"WHERE batch_id = '{batch_id}'"
    )
    try:
        result = _execute_query(query)
    except Exception:
        _logger.error(
            f"Failed to fetch received_time for batch_id={batch_id}",
            exc_info=True,
        )
        return None

    if not result or not result[0] or result[0][0] is None:
        return None

    value = result[0][0]
    if isinstance(value, datetime.datetime):
        return value
    try:
        return datetime.datetime.fromisoformat(str(value))
    except Exception:
        _logger.warning(
            f"Could not parse received_time={value!r} for batch_id={batch_id}; "
            f"FCFS will treat this batch as having unknown arrival time"
        )
        return None


def sort_jobs_fcfs(jobs: list[dict]) -> list[dict]:
    """Sort jobs ascending by received_time; None-time jobs last (stable)."""
    def _key(job: dict):
        rt = job.get("_received_time")
        return (rt is None, rt or datetime.datetime.min)
    return sorted(jobs, key=_key)


def fetch_new_jobs() -> list[dict]:
    """Discover ready batches under /data/ and assemble job dicts.

    For every (partner, folder) pair under /data/ where the partner is
    a valid Ovid partner directory and the folder has been quiet for
    longer than MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES, return
    a job dict ready for run_batch().

    Newly-discovered batches that don't yet have a data_batch row are
    inserted (status='NEW') and immediately considered for inclusion
    in the returned list if they are also ready.
    """
    # Walk the host-side partner-data root. The legacy in-container
    # scheduler used to walk '/data' because Docker's bind mount made
    # '/data' == '/data/inboxarchive' inside the container; the
    # orchestrator-mode scheduler runs on the host where that mapping
    # does not exist, so we walk the real path directly.
    data_path = HOST_INPUT_DIR
    partners_root = os.path.join(_REPO_ROOT, 'partners')

    partners_list = [
        name.lower() for name in os.listdir(partners_root)
        if os.path.isdir(os.path.join(partners_root, name))
    ]

    jobs: list[dict] = []

    for partner_folder in os.listdir(data_path):
        partner_folder_path = os.path.join(data_path, partner_folder)
        if not os.path.isdir(partner_folder_path):
            continue
        if partner_folder.lower() not in partners_list:
            continue

        for data_folder in os.listdir(partner_folder_path):
            batch_id = f"{partner_folder.lower()}_{data_folder}"

            # Gate out phantom batches: folders that are empty or contain
            # only archives (e.g. the empty YYYYMMDD.tar.gz an external
            # nightly cron drops into UFH) hold no ETL-usable input, so
            # they must never become a NEW batch. Without this check they
            # run a full no-op ETL and email an all-not_found report every
            # night. See _folder_has_eligible_input() (2026-09-08 fix).
            data_folder_path = os.path.join(partner_folder_path, data_folder)
            if not os.path.isdir(data_folder_path):
                continue
            if not _folder_has_eligible_input(data_folder_path):
                _logger.info(
                    f"Skipping candidate batch {batch_id}: no eligible "
                    f"csv/tsv/txt input files in {data_folder_path} "
                    f"(empty or archive-only)"
                )
                continue

            if batch_exist_in_scheduler_db(batch_id):
                if batch_ready_to_process(batch_id):
                    jobs.append(_build_job_dict(partner_folder, data_folder, batch_id))
            else:
                add_new_batch_to_db(
                    partner_folder.lower(), data_folder, partner_folder
                )
                if batch_ready_to_process(batch_id):
                    jobs.append(_build_job_dict(partner_folder, data_folder, batch_id))

    return jobs


def _resolve_partner_input_tables(partner: str) -> list[str]:
    """Return the ``-t``/``ovid_tables`` argv values for ``partner``.

    Reads the optional ``INPUT_TABLES`` attribute from
    ``partners/<partner>/partner_settings.py``. When present and non-empty,
    every CDM-table-iterating pipeline stage (format, dedup, map, fix,
    upload, mapping_gap, monthly_count) is restricted to those tables.
    When absent, returns ``["all"]`` to preserve the historical behavior
    every other partner relies on.

    The primary use case is hash-token-only partners (JHS, 2026-08-03).
    Without this restriction the shared ``mapping_scripts/pcornet_mappers/``
    step fires all 26 mappers per batch and logs 25 spurious
    ``deduplicated file not found`` failures for the tables the partner
    does not ship. Ordering is preserved as declared in
    ``partner_settings.py``.

    Args:
        partner: Lowercase partner code (e.g. ``"jhs"``).

    Returns:
        A list of argv-ready table names to pass to the downstream
        ``ovid etl`` / ``ovid tools`` / ``ovid dq_checks`` / ``ovid analysis``
        subprocesses as their ``-t`` arguments.
    """
    try:
        partner_settings = importlib.import_module(
            f"partners.{partner}.partner_settings"
        )
    except Exception:
        _logger.error(
            f"Could not import partner_settings for '{partner}' while "
            f"resolving INPUT_TABLES; defaulting to ['all']",
            exc_info=True,
        )
        return ["all"]

    input_tables = getattr(partner_settings, "INPUT_TABLES", None)
    if not input_tables:
        return ["all"]

    # Defensive: coerce to a list of stripped strings, drop empties.
    if isinstance(input_tables, str):
        input_tables = [input_tables]
    normalized = [str(name).strip() for name in input_tables if str(name).strip()]
    return normalized or ["all"]


def _build_job_dict(
    partner_folder: str, data_folder: str, batch_id: str
) -> dict[str, Any]:
    """Build the job dict consumed by run_batch().

    Honors the optional ``INPUT_TABLES`` attribute in the partner's
    ``partner_settings.py`` via :func:`_resolve_partner_input_tables`.
    Hash-token-only partners (JHS as of 2026-08-03) use that hook to
    prevent the ETL pipeline from iterating all 26 pcornet mappers per
    batch. Every other partner has no ``INPUT_TABLES`` attribute and
    receives the historical ``"all"`` value unchanged.
    """
    partner_lower = partner_folder.lower()
    target_db = f"char_{partner_lower}"
    input_tables = _resolve_partner_input_tables(partner_lower)
    return {
        "ovid_folder": data_folder,
        "ovid_tables": input_tables,
        "ovid_jobs": "all",
        "ovid_partner": partner_lower,
        "ovid_partner_folder": partner_folder,
        "ovid_target_db": target_db,
        "ovid_db_type": SCHEDULER_DB_TYPE,
        "ovid_db_server": SCHEDULER_DB_SERVER,
        "ovid_db": target_db,
        "ovid_input_data_folder_name": f"/data/{partner_folder}/",
        "_received_time": get_batch_received_time(batch_id),
    }


# ─── Partner_settings resolution helpers (unchanged from legacy) ─────────


def _get_partner_attr(partner: str, attr: str, default: str = "") -> str:
    """Read a single attribute from partners/<partner>/partner_settings.py."""
    try:
        mod = importlib.import_module(f"partners.{partner}.partner_settings")
        value = getattr(mod, attr, None)
        return str(value) if value is not None else default
    except Exception:
        _logger.error(
            f"Could not read {attr} for partner '{partner}'", exc_info=True
        )
        return default


def _get_partner_upload_type(partner: str) -> str:
    return _get_partner_attr(partner, "DEFAULT_UPLOAD_TYPE", "N/A") or "N/A"


def _get_partner_merge_database(partner: str) -> str:
    return _get_partner_attr(partner, "MERGE_DATABASE_NAME", "")


def _get_partner_merge_schema(partner: str) -> str:
    return _get_partner_attr(partner, "MERGE_SCHEMA_NAME", "")


def resolve_cdm_count_location(
    partner: str,
    folder: str,
    default_db: str,
) -> tuple[str, str]:
    """Resolve where a partner's CDM tables land, for email row counting.

    The scheduler's completion email counts rows per CDM table by querying
    Snowflake. Where those tables live depends on the partner's upload type:

    * ``no_merge`` (full-refresh) partners write bare-named CDM tables into
      the per-run ``OVID_<folder>`` schema of the partner database. See
      ``ovidFunctions.db_upload_snowflake``.
    * ``merge`` partners write into a per-run staging schema
      ``OVID_<folder>_MERGED_WITH_<merge_db>_<merge_schema>`` and then merge
      the rows into the authoritative final target ``<merge_db>.<merge_schema>``.
      See ``commonFunctions.db_upload_snowflake`` (the ``upload_type == 'merge'``
      branch that builds ``db_schema_name``). Counting the plain ``OVID_<folder>``
      schema for a merge partner returns nothing, which previously produced a
      spurious all-``not_found`` table report even on fully successful runs.

    For merge partners this returns the final merge target so the email
    reflects the authoritative, queryable location of the CDM data.

    Args:
        partner: Lower-case partner slug (e.g. ``"tmh"``).
        folder: The batch data folder / date token (e.g. ``"20260911"``).
        default_db: Fallback database when the partner defines no merge
            database (used for the ``no_merge`` path).

    Returns:
        A ``(database, schema)`` tuple identifying where to count CDM rows.
    """
    upload_type = _get_partner_upload_type(partner).strip().lower()
    if upload_type == "merge":
        merge_db = _get_partner_merge_database(partner) or default_db
        merge_schema = _get_partner_merge_schema(partner)
        if merge_schema:
            return merge_db, merge_schema.upper()
        # Merge partner without an explicit schema: fall back to the per-run
        # staging schema so the email still reports this batch's rows.
        return merge_db, f"OVID_{folder}".upper()
    return default_db, f"OVID_{folder}".upper()


# ─── Email + table-list helpers (unchanged from legacy) ──────────────────


def resolve_all_tables(partner: str) -> list[str]:
    """Resolve 'all' to the partner's CDM table list via tables_specs.csv."""
    try:
        partner_settings = importlib.import_module(
            f"partners.{partner}.partner_settings"
        )
        mappers_type = getattr(partner_settings, "mappers_type", "pcornet_mappers")
    except Exception:
        _logger.error(
            f"Could not import partner_settings for '{partner}'; "
            f"falling back to PCORNET_PRIMARY_KEYS table list",
            exc_info=True,
        )
        return list(PCORNET_PRIMARY_KEYS.keys())

    specs_path = os.path.join(
        _REPO_ROOT, "mapping_scripts", mappers_type, "tables_specs.csv"
    )
    if not os.path.exists(specs_path):
        _logger.warning(
            f"tables_specs.csv not found at {specs_path}; "
            f"falling back to PCORNET_PRIMARY_KEYS table list"
        )
        return list(PCORNET_PRIMARY_KEYS.keys())

    try:
        seen: list[str] = []
        seen_set: set[str] = set()
        with open(specs_path, "r", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            for row in reader:
                name = (row.get("TABLE_NAME") or "").strip().upper()
                if name and name not in seen_set:
                    seen.append(name)
                    seen_set.add(name)
        return seen or list(PCORNET_PRIMARY_KEYS.keys())
    except Exception:
        _logger.error(
            f"Failed to read tables_specs.csv at {specs_path}; "
            f"falling back to PCORNET_PRIMARY_KEYS table list",
            exc_info=True,
        )
        return list(PCORNET_PRIMARY_KEYS.keys())


def get_snowflake_row_counts(
    db_server: str,
    db: str,
    schema: str,
    tables: list[str],
) -> dict[str, Optional[int]]:
    """Query SHOW TABLES + COUNT(*) per table; missing tables return None."""
    counts: dict[str, Optional[int]] = {t.upper(): None for t in tables}
    if not tables:
        return counts

    conn = None
    cursor = None
    try:
        conn = _get_sf_connection(db_server)
        cursor = conn.cursor()

        existing: set[str] = set()
        try:
            cursor.execute(f"SHOW TABLES IN SCHEMA {db}.{schema}")
            for row in cursor.fetchall():
                if len(row) > 1 and row[1]:
                    existing.add(str(row[1]).upper())
        except Exception:
            _logger.warning(
                f"Could not list tables in {db}.{schema} "
                f"(schema may not exist yet); row counts will be 'n/a'",
                exc_info=True,
            )
            return counts

        for table in tables:
            tbl_upper = table.upper()
            if tbl_upper not in existing:
                continue
            try:
                cursor.execute(f"SELECT COUNT(*) FROM {db}.{schema}.{tbl_upper}")
                result = cursor.fetchone()
                if result and result[0] is not None:
                    counts[tbl_upper] = int(result[0])
            except Exception:
                _logger.warning(
                    f"Failed to count rows in {db}.{schema}.{tbl_upper}",
                    exc_info=True,
                )
    except Exception:
        _logger.error(
            f"Could not open Snowflake connection for row counts "
            f"({db_server}/{db}/{schema})",
            exc_info=True,
        )
    finally:
        try:
            if cursor is not None:
                cursor.close()
        except Exception:
            pass
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

    return counts


def collect_errors_from_log(
    log_file_path: Optional[str],
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    max_lines: int,
) -> list[str]:
    """Scan the OVID log file for ERROR / FAILURE lines in a time window."""
    if not log_file_path or not os.path.exists(log_file_path):
        return []

    window_start = start_time - datetime.timedelta(seconds=5)
    window_end = end_time + datetime.timedelta(seconds=5)

    collected: list[str] = []
    in_traceback_context = False

    try:
        with open(log_file_path, "r", encoding="utf-8", errors="replace") as fh:
            for raw_line in fh:
                line = raw_line.rstrip("\n")
                match = _LOG_LINE_RE.match(line)

                if match:
                    in_traceback_context = False
                    try:
                        ts = datetime.datetime.strptime(
                            match.group("ts"), "%Y-%m-%d %H:%M:%S"
                        )
                    except ValueError:
                        continue
                    if not (window_start <= ts <= window_end):
                        continue

                    level = match.group("level")
                    is_error_level = level in ("ERROR", "CRITICAL")
                    is_failure = "FAILURE:" in line
                    if is_error_level or is_failure:
                        collected.append(line)
                        in_traceback_context = True
                        if len(collected) >= max_lines:
                            break
                else:
                    if in_traceback_context and line.strip():
                        collected.append(line)
                        if len(collected) >= max_lines:
                            break
    except Exception:
        _logger.error(
            f"Failed to read log file {log_file_path} for error collection",
            exc_info=True,
        )
        return []

    return collected


# ─── Raw-upload helpers (unchanged from legacy, just relocated) ──────────


def _resolve_email_recipients() -> list[str]:
    """Env var > settings.py for SCHEDULER_EMAIL_RECIPIENTS."""
    raw = os.environ.get("OVID_EMAIL_RECIPIENTS", "").strip()
    if raw:
        addrs = [r.strip() for r in raw.split(",") if r.strip()]
        if addrs:
            return addrs
    return list(SCHEDULER_EMAIL_RECIPIENTS)


def _resolve_email_enabled() -> bool:
    """Env var > settings.py for SCHEDULER_EMAIL_ENABLED."""
    raw = os.environ.get("OVID_EMAIL_DISABLED", "").strip().lower()
    if raw in ("1", "true", "yes"):
        return False
    return bool(SCHEDULER_EMAIL_ENABLED)


def _resolve_raw_upload_enabled() -> bool:
    """Env var > settings.py for SCHEDULER_RAW_UPLOAD_ENABLED."""
    raw = os.environ.get("OVID_RAW_UPLOAD_DISABLED", "").strip().lower()
    if raw in ("1", "true", "yes"):
        return False
    return bool(SCHEDULER_RAW_UPLOAD_ENABLED)


def _sanitize_raw_table_name(file_path: str) -> str:
    """Mirror of pipelines/tools.py:_file_to_table_name (for email row counts)."""
    base = os.path.basename(file_path)
    stem, _ = os.path.splitext(base)
    raw_name = stem.upper()
    sanitized = re.sub(r"[^A-Z0-9_]", "_", raw_name)
    sanitized = re.sub(r"_+", "_", sanitized).strip("_")
    if not sanitized:
        sanitized = "T_UNNAMED"
    elif sanitized[0].isdigit():
        sanitized = "T_" + sanitized
    return sanitized


def _list_raw_upload_files(raw_input_dir: str) -> list[str]:
    """List csv/tsv/txt files in raw_input_dir (mirror of tools.py logic).

    Args:
        raw_input_dir: Absolute host-side path to the folder to enumerate.

    Returns:
        Sorted list of absolute paths to ``.csv``/``.tsv``/``.txt`` files
        directly inside ``raw_input_dir``. Returns an empty list if the
        path is missing, is not a directory, or cannot be read (e.g. a
        permission-denied folder): from the scheduler's perspective an
        unreadable folder holds no eligible input, so discovery skips it
        rather than aborting the whole ``fetch_new_jobs()`` scan.
    """
    # Keep extension list inline (was previously imported from
    # commonFunctions.RAW_DV_ALLOWED_EXTENSIONS, but that import path
    # is heavier than we need on the host orchestrator).
    allowed = {'.csv', '.tsv', '.txt'}

    if not os.path.isdir(raw_input_dir):
        return []

    try:
        entries = sorted(os.listdir(raw_input_dir))
    except OSError:
        # PermissionError (Errno 13) and other listing failures must not
        # crash discovery. Some archived partner folders under
        # /data/inboxarchive/ are owned by root with no world-read bit;
        # walking one of them used to abort the entire scan and starve
        # every partner alphabetically after it (e.g. uci/umi/umn/usf/vrt
        # after ufh). Log and treat as no eligible input.
        _logger.warning(
            f"Could not list raw input files in {raw_input_dir}; "
            f"treating as no eligible input",
            exc_info=True,
        )
        return []

    matched: list[str] = []
    for entry in entries:
        full_path = os.path.join(raw_input_dir, entry)
        if not os.path.isfile(full_path):
            continue
        ext = os.path.splitext(entry)[1].lower()
        if ext not in allowed:
            continue
        matched.append(full_path)
    return matched


# Archive extensions that a batch folder may contain but which the ETL
# pipeline never reads directly (it consumes extracted csv/tsv/txt files).
# A folder whose ONLY contents are these archives — or that is empty — is
# not a real batch and must not be dispatched.  See
# _folder_has_eligible_input() and the 2026-09-08 phantom-batch fix.
_ARCHIVE_EXTENSIONS = ('.tar.gz', '.tgz', '.tar', '.zip', '.gz')


def _folder_has_ready_marker(host_folder_path: str) -> bool:
    """Return True if the batch folder holds an event-driven ready marker.

    The archive/upload process signals that a batch is fully written by
    dropping a marker file (any name in ``SCHEDULER_READY_MARKER_FILENAMES``)
    into the batch folder as its last step. When present, the scheduler
    dispatches the batch immediately instead of waiting the
    ``MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES`` quiet-period
    fallback. The marker filenames are dot-prefixed / non-data extensions
    so ``_list_raw_upload_files`` (csv/tsv/txt only) never treats them as
    ETL input.

    Args:
        host_folder_path: Absolute host-side path to the candidate batch
            folder (already translated via ``_host_path_for_partner_data``
            if it originated as a container-relative path).

    Returns:
        True if ready-marker signaling is enabled and at least one marker
        file is present in the folder; False otherwise (including when the
        folder is missing or unreadable).
    """
    if not SCHEDULER_READY_MARKER_ENABLED:
        return False
    if not os.path.isdir(host_folder_path):
        return False
    for marker_name in SCHEDULER_READY_MARKER_FILENAMES:
        marker_path = os.path.join(host_folder_path, marker_name)
        try:
            if os.path.isfile(marker_path):
                return True
        except OSError:
            # A stat failure on one candidate marker must not abort the
            # scan; log and keep checking the remaining marker names.
            _logger.warning(
                f"Could not stat ready-marker {marker_path}; "
                f"treating as absent",
                exc_info=True,
            )
    return False


def _folder_has_eligible_input(host_folder_path: str) -> bool:
    """Return True if a batch folder contains at least one ETL-usable file.

    The scheduler treats every subfolder of a partner directory as a
    candidate batch, but an external nightly cron drops empty
    ``YYYYMMDD.tar.gz`` archives into fresh UFH folders (observed
    2026-09-08). Those archives are never extracted by the ETL, so the
    pipeline produces zero tables, marks the batch PROCESSED, and emails
    an all-``not_found`` report every night. This helper is the gate that
    keeps such phantom folders from ever becoming a NEW batch.

    A folder is considered to have eligible input if it contains at least
    one ``.csv``/``.tsv``/``.txt`` file (the extensions the ETL and
    raw_upload stages actually consume). Folders that are empty, or that
    contain only archives (``.tar.gz``/``.tgz``/``.tar``/``.zip``/``.gz``)
    with no extracted data files, return False.

    Args:
        host_folder_path: Absolute host-side path to the candidate batch
            folder (already translated via _host_path_for_partner_data if
            it originated as a container-relative path).

    Returns:
        True if the folder holds at least one csv/tsv/txt file; False if
        the folder is missing, empty, or archive-only.
    """
    if _list_raw_upload_files(host_folder_path):
        return True

    if not os.path.isdir(host_folder_path):
        return False

    # No eligible csv/tsv/txt file was found. Distinguish an
    # archive-only / empty folder (the phantom case we suppress) from a
    # folder holding some other unrecognised-but-real file, which we log
    # loudly so an operator can investigate rather than silently drop.
    non_archive_files: list[str] = []
    try:
        for entry in os.listdir(host_folder_path):
            full_path = os.path.join(host_folder_path, entry)
            if not os.path.isfile(full_path):
                continue
            lowered = entry.lower()
            if any(lowered.endswith(ext) for ext in _ARCHIVE_EXTENSIONS):
                continue
            non_archive_files.append(entry)
    except OSError:
        _logger.error(
            f"Could not inspect candidate batch folder {host_folder_path} "
            f"for eligible input; treating as ineligible",
            exc_info=True,
        )
        return False

    if non_archive_files:
        _logger.warning(
            f"Batch folder {host_folder_path} has no csv/tsv/txt input but "
            f"contains {len(non_archive_files)} other file(s) "
            f"({non_archive_files[:5]}); treating as ineligible — inspect "
            f"manually if this folder should have been processed"
        )
    return False


# ─── Subprocess execution per slot ────────────────────────────────────────


def _batch_log_path(
    slot_index: int, batch_id: str, started_at: datetime.datetime
) -> str:
    """Return the per-batch log file path for ovid-watch to tail.

    Per the 2026-06-25 redesign, the slot index is embedded directly in
    the filename so ``ovid-watch`` can label its pages with both the
    orchestrator's slot pool identity (``slot 1`` / ``slot 2`` / ...) AND
    the unique batch identifier. The full pattern is::

        systemd-scheduler-batch-slot{N}-{batch_id}-{YYYYMMDD_HHMMSS}.log

    Per the original 2026-06-16 design, every batch still gets its own
    permanent log file — the file is created fresh at batch start (no
    reuse, no truncation), so an operator who has ``ovid-watch`` open
    and is following a particular batch's page will never see that
    page's frames silently disappear when a slot is reused for a
    different batch (the "daemon writes to the same window" bug).

    Pre-2026-06-25 files on disk still match the legacy pattern
    ``systemd-scheduler-batch-{batch_id}-{timestamp}.log`` (no
    ``slot{N}-`` segment); ``ovid-watch`` recognises both via separate
    regex patterns and labels legacy pages as ``{batch_id} [legacy]``.

    The timestamp resolution is to the second; same-second collisions
    on the same batch_id are not realistic in production (batches do
    not retry within one second of each other).

    Args:
        slot_index: 1-based orchestrator slot index that runs this
            batch. Embedded in the filename so the viewer can render
            ``slot N`` in its page label without IPC back to the daemon.
        batch_id: Canonical batch identifier ``{partner}_{folder}``.
        started_at: Wall-clock time the batch began on its slot.

    Returns:
        Absolute path to the batch's log file.
    """
    os.makedirs(SCHEDULER_SLOT_LOG_DIR, exist_ok=True)
    timestamp = started_at.strftime("%Y%m%d_%H%M%S")
    return os.path.join(
        SCHEDULER_SLOT_LOG_DIR,
        f"systemd-scheduler-batch-slot{slot_index}-{batch_id}-{timestamp}.log",
    )


def _open_batch_log(log_path: str) -> None:
    """Create the per-batch log file with friendly permissions.

    Called once at the very start of a batch (before any subprocess
    output flows in) so that the file exists with mode 0o644 and is
    readable by ovid-watch readers running as a different uid (unlikely
    in practice but harmless to enforce).
    """
    try:
        with open(log_path, 'w', encoding='utf-8') as fh:
            fh.truncate(0)
        try:
            os.chmod(log_path, 0o644)
        except OSError:
            pass
    except OSError:
        _logger.error(
            f"Could not create batch log {log_path}; subprocess output "
            f"may be unreachable",
            exc_info=True,
        )


def _cascade_terminate_slots(grace_seconds: float = 30.0) -> int:
    """Send SIGTERM to every running slot subprocess's process group.

    Called from the orchestrator's shutdown path (KeyboardInterrupt /
    SIGTERM) so that `systemctl stop ovid-scheduler` is authoritative —
    no orphaned ETL or raw_upload subprocesses survive past the daemon's
    deactivation. Each slot Popen was launched with
    ``start_new_session=True`` so its pid == pgid; SIGTERM to the pgid
    delivers to the entire subtree (docker-exec, bash, spark-submit,
    java, python).

    For each tracked slot Popen:
      1. ``os.killpg(proc.pid, SIGTERM)``
      2. Wait up to ``grace_seconds / N`` for it to exit on its own.
      3. If still alive, ``os.killpg(proc.pid, SIGKILL)``.

    The function is best-effort: any individual termination failure is
    logged but does not block the others. Returns the number of slot
    subprocesses successfully signalled (for logging).

    Note: this also tears down any Docker compose project the slot
    subprocess had spawned, because the slot's `ovid etl` host-side
    Python process (the session leader) catches SIGTERM via the
    standard `ovid.py` signal handler at module load — which runs
    `_shutdown_cluster()` and `docker compose down`. So in the common
    case we don't even need SIGKILL: SIGTERM gives `ovid.py` a chance
    to clean up its instance compose project before exiting.
    """
    with _active_slot_procs_lock:
        snapshot = dict(_active_slot_procs)

    if not snapshot:
        return 0

    _logger.info(
        f"Cascade-stop: sending SIGTERM to {len(snapshot)} in-flight "
        f"slot subprocess(es)"
    )

    # Phase 1: dispatch SIGTERM to every pgid.
    signalled: list[tuple[int, "subprocess.Popen"]] = []
    for slot_index, proc in snapshot.items():
        if proc.poll() is not None:
            continue  # already finished
        try:
            pgid = os.getpgid(proc.pid)
        except (ProcessLookupError, OSError):
            continue
        try:
            os.killpg(pgid, signal.SIGTERM)
            _logger.info(
                f"Cascade-stop: SIGTERM -> slot {slot_index} pgid={pgid} "
                f"(pid={proc.pid})"
            )
            signalled.append((slot_index, proc))
        except (ProcessLookupError, PermissionError, OSError) as exc:
            _logger.warning(
                f"Cascade-stop: SIGTERM failed for slot {slot_index} "
                f"pgid={pgid}: {exc!r}"
            )

    # Phase 2: bounded wait for graceful exit. Per-slot deadline so
    # one slow exit doesn't starve the others.
    per_slot_deadline = max(grace_seconds / max(len(signalled), 1), 5.0)
    for slot_index, proc in signalled:
        try:
            proc.wait(timeout=per_slot_deadline)
        except subprocess.TimeoutExpired:
            try:
                pgid = os.getpgid(proc.pid)
                os.killpg(pgid, signal.SIGKILL)
                _logger.warning(
                    f"Cascade-stop: SIGTERM grace expired for slot "
                    f"{slot_index} (pid={proc.pid}); SIGKILL'd"
                )
            except (ProcessLookupError, PermissionError, OSError):
                pass

    return len(signalled)


def _ovid_wrapper_path() -> str:
    """Resolve the host-side `ovid` wrapper to invoke for ETL/tools.

    Prefer /usr/local/bin/ovid (installed by install_systemd_service.sh).
    Fall back to `python3 ovid.py` from the repo root if the wrapper is
    not on PATH (e.g. dev environments where the installer hasn't run).
    """
    if os.access(_OVID_WRAPPER_DEFAULT, os.X_OK):
        return _OVID_WRAPPER_DEFAULT
    return f"{sys.executable} {os.path.join(_REPO_ROOT, 'ovid.py')}"


def _run_subcommand_to_slot_log(
    slot_index: int,
    log_path: str,
    args: list[str],
    log_header: str,
) -> int:
    """Run a subcommand and append its stdout/stderr to the batch's log.

    The batch log file is the file `ovid-watch` tails. We open it in
    append mode (the file was created fresh at batch start by
    ``_open_batch_log()``) and pass the open file as both stdout and
    stderr to subprocess.Popen, so the raw ANSI byte stream from the
    OVID progress display is preserved exactly.

    A short header line + visual separator is written before each
    subcommand begins so the operator can tell ETL output from
    raw_upload output.

    Args:
        slot_index: 1-based slot identifier (used only for the
            ``[slot N]`` prefix in info logs; the log file path is
            now per-batch, not per-slot).
        log_path: Absolute path to the batch's log file (the same
            file is shared by the ETL stage and the raw_upload stage
            so ovid-watch shows their frames in one continuous page).
        args: Argv list for the subcommand (already shell-safe).
        log_header: Human-readable banner written before the subprocess.

    Returns:
        The subprocess returncode.
    """
    try:
        with open(log_path, 'a', encoding='utf-8') as log_fh:
            log_fh.write(f"\n{'=' * 78}\n{log_header}\n{'=' * 78}\n")
            log_fh.flush()
    except OSError:
        _logger.error(
            f"Could not write banner to batch log {log_path}",
            exc_info=True,
        )

    _logger.info(
        f"slot {slot_index}: launching subprocess: {' '.join(args)} "
        f"(log: {log_path})"
    )

    try:
        with open(log_path, 'ab') as log_fh:
            # start_new_session=True decouples the child process from the
            # orchestrator's session/process group so a stray SIGINT into
            # the orchestrator (e.g. operator Ctrl+C from journalctl
            # follow) does not kill in-flight slot subprocesses. Each
            # batch must be allowed to finish gracefully.
            #
            # The orchestrator IS able to terminate its own slot
            # subprocesses on a deliberate shutdown (systemctl stop) by
            # explicitly calling os.killpg() against each tracked Popen
            # — see _cascade_terminate_slots(). The session/pgid set up
            # here is the handle that lets that work cleanly.
            proc = subprocess.Popen(
                args,
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                stdin=subprocess.DEVNULL,
                start_new_session=True,
            )
            # Register this Popen so a deliberate shutdown can SIGTERM
            # the entire process group via os.killpg(proc.pid, SIGTERM).
            with _active_slot_procs_lock:
                _active_slot_procs[slot_index] = proc
            try:
                return proc.wait()
            finally:
                with _active_slot_procs_lock:
                    # Only clear if it's still the same Popen we registered;
                    # defensive in case another stage has already overwritten
                    # the slot entry (it shouldn't, but cheap insurance).
                    if _active_slot_procs.get(slot_index) is proc:
                        _active_slot_procs.pop(slot_index, None)
    except FileNotFoundError:
        _logger.error(
            f"slot {slot_index}: command not found: {args[0]}",
            exc_info=True,
        )
        return 127
    except Exception:
        _logger.error(
            f"slot {slot_index}: subprocess raised", exc_info=True
        )
        return 1


# ─── Per-batch end-to-end execution (runs inside a slot) ─────────────────


def run_batch(slot_index: int, job: dict[str, Any]) -> None:
    """Run one batch end-to-end on the given slot.

    This function executes inside a worker thread of the orchestrator's
    ThreadPoolExecutor.  It is the host-side equivalent of the legacy
    scheduler's `run_job()` function but spawns subprocesses through
    the standard `ovid` CLI (preserving 100% of the existing instance
    isolation, terminal-lock, and per-instance logging behaviour) rather
    than via in-container `docker exec` + spark-submit.

    Steps:
      1. Mark batch PROCESSING in data_batch.
      2. Compute the per-batch log file path and create it fresh
         so ovid-watch shows a brand-new page for this batch (and
         the previous batch's page on disk remains intact).
      3. Spawn `ovid etl ...` for the batch and wait.
      4. Capture the ETL returncode -> data_batch.status (PROCESSED/ERROR).
      5. Spawn `ovid tools -j raw_upload ...` (always, success or fail).
      6. Compose batch_info and dispatch the completion email.

    Args:
        slot_index: 1-based slot identifier (used for the "[slot N]"
            prefix in info logs only; the log file path is per-batch
            since 2026-06-16, not per-slot).
        job: Job dict from fetch_new_jobs().
    """
    partner = job["ovid_partner"]
    folder = job["ovid_folder"]
    partner_folder = job.get("ovid_partner_folder") or partner
    batch_id = f"{partner}_{folder}"

    instance_id_for_email = f"slot-{slot_index}"

    # Claim the slot.
    with _slot_lock:
        _active_slots[slot_index] = batch_id

    processing_start_dt = datetime.datetime.now()
    batch_final_status = 'ERROR'
    raw_upload_result: Optional[dict[str, Any]] = None

    try:
        update_batch_field(partner.upper(), folder.upper(), 'status', 'PROCESSING')
        update_batch_field(
            partner.upper(), folder.upper(),
            'processing_start', _now_eastern_str()
        )
    except Exception:
        _logger.error(
            f"slot {slot_index}: failed to mark {batch_id} PROCESSING; "
            f"continuing anyway",
            exc_info=True,
        )

    # Per-batch log file (NOT per-slot — see _batch_log_path() docstring).
    # Both the ETL stage and the raw_upload stage append to this same
    # file so ovid-watch shows their frames in one continuous page.
    batch_log_path = _batch_log_path(slot_index, batch_id, processing_start_dt)
    _open_batch_log(batch_log_path)

    _logger.info(
        f"[slot {slot_index}] starting batch {batch_id} "
        f"(received_time={job.get('_received_time')}) -> {batch_log_path}"
    )

    # ─── ETL stage ───────────────────────────────────────────────────
    upload_type = _get_partner_upload_type(partner)
    merge_db = _get_partner_merge_database(partner) or "NA"
    merge_schema = _get_partner_merge_schema(partner) or "NA"
    if upload_type in (None, "", "N/A"):
        upload_type = "no_merge"

    ovid_cmd = _ovid_wrapper_path()
    # The wrapper may be either an executable path or a "python3 ovid.py"
    # string; shlex.split handles both correctly.
    #
    # `ovid_tables` is a list produced by _build_job_dict via
    # _resolve_partner_input_tables. It is ["all"] for every partner that
    # ships a full PCORnet CDM (24 of 25 partners as of 2026-08-03) and
    # ["hash_token"] for JHS. Legacy code paths that stored a bare string
    # are tolerated by wrapping non-list values in a single-element list.
    raw_tables = job.get("ovid_tables", "all")
    if isinstance(raw_tables, str):
        tables_argv = [raw_tables]
    else:
        tables_argv = list(raw_tables) if raw_tables else ["all"]
    etl_args = shlex.split(ovid_cmd) + [
        "etl",
        "-p", partner,
        "-j", "all",
        "-t", *tables_argv,
        "-f", f"{partner_folder}/{folder}",
        "-dt", job.get("ovid_db_type", SCHEDULER_DB_TYPE),
        "-s", job.get("ovid_db_server", SCHEDULER_DB_SERVER),
        "-db", job.get("ovid_db", f"char_{partner}"),
        "-ut", upload_type,
        "-mdb", merge_db,
        "-ms", merge_schema,
        "-wc", str(SCHEDULER_WORKERS_PER_SLOT),
    ]

    etl_rc = _run_subcommand_to_slot_log(
        slot_index,
        batch_log_path,
        etl_args,
        log_header=(
            f"OVID ETL  |  slot {slot_index}  |  batch {batch_id}  |  "
            f"started {processing_start_dt.isoformat(timespec='seconds')}  |  "
            f"upload_type={upload_type}"
        ),
    )

    _logger.info(
        f"[slot {slot_index}] ETL spark-submit returned for {batch_id}: "
        f"returncode={etl_rc}"
    )

    try:
        if etl_rc == 0:
            update_batch_field(partner.upper(), folder.upper(), 'status', 'PROCESSED')
            batch_final_status = 'PROCESSED'
        else:
            update_batch_field(partner.upper(), folder.upper(), 'status', 'ERROR')
            batch_final_status = 'ERROR'
        update_batch_field(
            partner.upper(), folder.upper(),
            'processing_end', _now_eastern_str()
        )
    except Exception:
        _logger.error(
            f"slot {slot_index}: failed to update final status for {batch_id}",
            exc_info=True,
        )

    # ─── Raw upload stage (always runs, success or fail, UNLESS shutdown) ──
    # If the operator triggered a cascade-stop (systemctl stop), the
    # ETL subprocess was just SIGTERM'd by _cascade_terminate_slots()
    # and we are unwinding. Do NOT spawn the raw_upload stage in that
    # case — it would try to launch another fresh cluster while the
    # operator's intent is to shut everything down.
    if _shutdown_requested.is_set():
        _logger.info(
            f"[slot {slot_index}] shutdown requested; skipping raw_upload "
            f"for {batch_id}"
        )
        raw_upload_result = {
            "enabled": True,
            "executed": False,
            "success": False,
            "returncode": None,
            "start": None,
            "end": None,
            "duration": datetime.timedelta(0),
            "schema": f"RAW_{folder.upper()}",
            "database": job.get("ovid_db"),
            "file_count": 0,
            "error_summary": "skipped: orchestrator shutdown requested",
        }
    else:
        try:
            raw_upload_result = run_raw_upload_stage(slot_index, batch_log_path, job)
        except Exception:
            _logger.error(
                f"slot {slot_index}: unhandled exception during raw_upload "
                f"for {batch_id}; ETL batch status preserved",
                exc_info=True,
            )
            raw_upload_result = {
                "enabled": True,
                "executed": True,
                "success": False,
                "returncode": None,
                "start": None,
                "end": None,
                "duration": datetime.timedelta(0),
                "schema": f"RAW_{folder.upper()}",
                "database": job.get("ovid_db"),
                "file_count": 0,
                "error_summary": "raw_upload helper raised",
            }

    # ─── Email notification (suppressed during shutdown) ─────────────
    # When the operator triggered a deliberate shutdown, the slot
    # subprocesses were SIGTERM'd mid-stage. Sending an "ERROR" email
    # for each of them would spam the recipients with notifications
    # for batches the operator KNOWINGLY killed. The data_batch rows
    # stay PROCESSING and get reset to ERROR by the next daemon
    # startup's reset_processing_to_error_on_startup() sweep — so
    # there is a real audit trail in Snowflake; we just don't email.
    processing_end_dt = datetime.datetime.now()
    if _shutdown_requested.is_set():
        _logger.info(
            f"[slot {slot_index}] shutdown requested; skipping completion "
            f"email for {batch_id}"
        )
    else:
        try:
            batch_info = build_batch_info(
                job,
                batch_final_status,
                processing_start_dt,
                processing_end_dt,
                instance_id_for_email,
                raw_upload_result=raw_upload_result,
            )
            if _batch_produced_no_output(batch_info):
                _logger.info(
                    f"[slot {slot_index}] batch {batch_id} PROCESSED but "
                    f"produced zero CDM tables and no raw uploads; "
                    f"suppressing completion email (phantom/empty batch). "
                    f"data_batch row left PROCESSED for audit."
                )
            else:
                send_batch_notification(batch_info)
        except Exception:
            _logger.error(
                f"slot {slot_index}: failed to send completion email for {batch_id}",
                exc_info=True,
            )

    # Free the slot.
    with _slot_lock:
        _active_slots[slot_index] = None

    _logger.info(
        f"[slot {slot_index}] finished batch {batch_id} "
        f"(status={batch_final_status}, "
        f"duration={(processing_end_dt - processing_start_dt)})"
    )


def run_raw_upload_stage(
    slot_index: int, batch_log_path: str, job: dict[str, Any]
) -> dict[str, Any]:
    """Run the post-ETL raw_upload stage for one batch on a slot.

    Spawns `ovid tools -j raw_upload -t all ...` against the same partner
    and folder, capturing the subprocess output to the same per-batch
    log file as ETL (so ovid-watch shows the raw_upload frames continuously
    after ETL frames within the same page). Returns a result dict in the
    same shape the legacy scheduler used so build_batch_info() / the
    email template don't change.

    Args:
        slot_index: 1-based slot identifier (used for the ``[slot N]``
            prefix in info logs only).
        batch_log_path: Per-batch log file path computed by ``run_batch()``
            via ``_batch_log_path()``. The raw_upload stage appends to
            this same file, sharing the page with the ETL stage.
        job: Job dict from fetch_new_jobs().
    """
    partner = job["ovid_partner"]
    partner_folder = job.get("ovid_partner_folder") or partner
    folder = job["ovid_folder"]

    enabled = _resolve_raw_upload_enabled()
    schema_name = f"RAW_{folder.upper()}"
    database = job.get("ovid_db")

    base_result: dict[str, Any] = {
        "enabled": enabled,
        "executed": False,
        "success": False,
        "returncode": None,
        "start": None,
        "end": None,
        "duration": datetime.timedelta(0),
        "schema": schema_name,
        "database": database,
        "file_count": 0,
        "error_summary": None,
    }

    if not enabled:
        _logger.info(
            f"[slot {slot_index}] raw_upload disabled for {partner}_{folder}"
        )
        return base_result

    # Host-side enumeration of the source files for this batch's
    # raw_upload metadata. Use HOST_INPUT_DIR; the in-container
    # `ovid tools` subprocess invoked below sees the same files via
    # its own Docker bind mount under /data/.
    raw_input_dir = f"{HOST_INPUT_DIR}/{partner_folder}/{folder}/"
    matched_files = _list_raw_upload_files(raw_input_dir)
    base_result["file_count"] = len(matched_files)

    if not matched_files:
        _logger.warning(
            f"[slot {slot_index}] raw_upload: no eligible csv/tsv/txt files "
            f"in {raw_input_dir}; skipping for batch {partner}_{folder}"
        )
        base_result["error_summary"] = "no eligible input files"
        return base_result

    ovid_cmd = _ovid_wrapper_path()
    tools_args = shlex.split(ovid_cmd) + [
        "tools",
        "-j", "raw_upload",
        "-p", partner,
        "-t", "all",
        "-f", f"{partner_folder}/{folder}",
        "-dt", job.get("ovid_db_type", SCHEDULER_DB_TYPE),
        "-s", job.get("ovid_db_server", SCHEDULER_DB_SERVER),
        "-db", database,
        "-wc", str(SCHEDULER_WORKERS_PER_SLOT),
    ]

    start_dt = datetime.datetime.now()
    base_result["start"] = start_dt
    base_result["executed"] = True

    rc = _run_subcommand_to_slot_log(
        slot_index,
        batch_log_path,
        tools_args,
        log_header=(
            f"OVID raw_upload  |  slot {slot_index}  |  batch {partner}_{folder}  |  "
            f"started {start_dt.isoformat(timespec='seconds')}  |  "
            f"{len(matched_files)} files -> {database}.{schema_name}"
        ),
    )

    end_dt = datetime.datetime.now()
    base_result["end"] = end_dt
    base_result["duration"] = end_dt - start_dt
    base_result["returncode"] = rc

    if rc == 0:
        base_result["success"] = True
        _logger.info(
            f"[slot {slot_index}] raw_upload succeeded for "
            f"{partner}_{folder} in {base_result['duration']}"
        )
    else:
        base_result["error_summary"] = f"returncode={rc}"
        _logger.error(
            f"[slot {slot_index}] raw_upload failed for "
            f"{partner}_{folder} (returncode={rc}); ETL status preserved"
        )

    return base_result


# ─── build_batch_info + send_batch_notification (unchanged from legacy) ──


def build_batch_info(
    job: dict[str, Any],
    status: str,
    processing_start: datetime.datetime,
    processing_end: datetime.datetime,
    instance_id: str,
    raw_upload_result: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """Assemble the batch_info dict used to render the notification email."""
    partner = job["ovid_partner"]
    folder = job["ovid_folder"]
    batch_id = f"{partner}_{folder}"

    tables_list = resolve_all_tables(partner)
    default_db = job.get("ovid_db", f"char_{partner}")
    count_db, schema_name = resolve_cdm_count_location(
        partner=partner,
        folder=folder,
        default_db=default_db,
    )
    row_counts = get_snowflake_row_counts(
        db_server=job.get("ovid_db_server", SCHEDULER_DB_SERVER),
        db=count_db,
        schema=schema_name,
        tables=tables_list,
    )

    tables_info: list[dict[str, Any]] = []
    for table in tables_list:
        tbl_upper = table.upper()
        count = row_counts.get(tbl_upper)
        if count is None:
            table_status = "not_found" if status == "PROCESSED" else "skipped"
        else:
            table_status = "uploaded"
        tables_info.append({
            "name": tbl_upper,
            "status": table_status,
            "row_count": count,
        })

    raw_tables_info: list[dict[str, Any]] = []
    if raw_upload_result and raw_upload_result.get("executed"):
        partner_folder = job.get("ovid_partner_folder") or partner
        # Host-side enumeration for the email's raw-tables summary;
        # see HOST_INPUT_DIR docs above.
        raw_input_dir = f"{HOST_INPUT_DIR}/{partner_folder}/{folder}/"
        raw_files = _list_raw_upload_files(raw_input_dir)
        raw_table_names = [_sanitize_raw_table_name(p) for p in raw_files]
        seen: set[str] = set()
        unique_names: list[str] = []
        for name in raw_table_names:
            if name not in seen:
                seen.add(name)
                unique_names.append(name)

        raw_schema = raw_upload_result.get("schema") or f"RAW_{folder.upper()}"
        raw_database = raw_upload_result.get("database") or job.get(
            "ovid_db", f"char_{partner}"
        )
        raw_counts = get_snowflake_row_counts(
            db_server=job.get("ovid_db_server", SCHEDULER_DB_SERVER),
            db=raw_database,
            schema=raw_schema,
            tables=unique_names,
        )

        raw_stage_succeeded = bool(raw_upload_result.get("success"))
        for name in unique_names:
            count = raw_counts.get(name)
            if count is None:
                raw_status = (
                    "not_found" if raw_stage_succeeded else "skipped"
                )
            else:
                raw_status = "uploaded"
            raw_tables_info.append({
                "name": name,
                "status": raw_status,
                "row_count": count,
            })

    log_file_path = get_log_file_path()
    errors = collect_errors_from_log(
        log_file_path=log_file_path,
        start_time=processing_start,
        end_time=processing_end,
        max_lines=SCHEDULER_EMAIL_MAX_ERROR_LINES,
    )

    base_stages = "format, deduplicate, map, fix, upload"
    if raw_upload_result is None:
        stages_str = base_stages
    elif not raw_upload_result.get("enabled"):
        stages_str = f"{base_stages} (raw_upload disabled)"
    elif not raw_upload_result.get("executed"):
        reason = raw_upload_result.get("error_summary") or "skipped"
        stages_str = f"{base_stages} (raw_upload: {reason})"
    elif raw_upload_result.get("success"):
        stages_str = f"{base_stages}, raw_upload"
    else:
        stages_str = f"{base_stages} (raw_upload failed)"

    return {
        "batch_id": batch_id,
        "partner": partner,
        "folder": folder,
        "host": socket.gethostname(),
        "status": status,
        "stages": stages_str,
        "database": job.get("ovid_db"),
        "db_type": job.get("ovid_db_type"),
        "db_server": job.get("ovid_db_server"),
        "upload_type": _get_partner_upload_type(partner),
        "processing_start": processing_start,
        "processing_end": processing_end,
        "duration": processing_end - processing_start,
        "instance_id": instance_id,
        "tables": tables_info,
        "raw_upload": raw_upload_result,
        "raw_tables": raw_tables_info,
        "errors": errors,
        "log_file_path": log_file_path,
    }


def _batch_produced_no_output(batch_info: dict[str, Any]) -> bool:
    """Return True if a PROCESSED batch produced zero CDM and zero raw tables.

    This is the degenerate "phantom batch" signature: the ETL ran to a
    clean exit (status PROCESSED) but every CDM table is ``not_found`` in
    Snowflake AND the raw_upload stage uploaded nothing. It occurs when a
    batch folder held no ETL-usable input — e.g. the empty
    ``YYYYMMDD.tar.gz`` an external cron drops into UFH nightly.

    :func:`fetch_new_jobs` already gates such folders out via
    :func:`_folder_has_eligible_input`, so this is a defence-in-depth
    safety net: if some future artifact slips past the gate (e.g. a
    folder with a stray non-CDM file), we still avoid spamming operators
    with an all-``not_found`` completion email. The ``data_batch`` row is
    left as PROCESSED so the audit trail is preserved.

    Only the truly-empty case is suppressed: a batch that produced ANY
    CDM table rows OR ANY raw-upload table still emails as before.

    Args:
        batch_info: The dict returned by :func:`build_batch_info`.

    Returns:
        True if the email should be suppressed (nothing was produced);
        False otherwise.
    """
    if batch_info.get("status") != "PROCESSED":
        return False

    tables = batch_info.get("tables") or []
    any_cdm_output = any(
        (t.get("status") == "uploaded") or (t.get("row_count") not in (None, 0))
        for t in tables
    )
    if any_cdm_output:
        return False

    raw_tables = batch_info.get("raw_tables") or []
    any_raw_output = any(
        (t.get("status") == "uploaded") or (t.get("row_count") not in (None, 0))
        for t in raw_tables
    )
    if any_raw_output:
        return False

    return True


def send_batch_notification(batch_info: dict[str, Any]) -> None:
    """Dispatch the batch completion email. Logs but never raises."""
    if not _resolve_email_enabled():
        _logger.info("Scheduler email notifications disabled; skipping send")
        return

    recipients = _resolve_email_recipients()
    if not recipients:
        _logger.warning(
            "No email recipients resolved (env var empty and "
            "SCHEDULER_EMAIL_RECIPIENTS empty); skipping send"
        )
        return

    _logger.info(
        f"Sending batch completion email to {recipients} "
        f"(source: {'env override' if os.environ.get('OVID_EMAIL_RECIPIENTS', '').strip() else 'settings.py default'})"
    )

    try:
        notifier = BatchNotifier(
            smtp_host=SCHEDULER_SMTP_HOST,
            smtp_port=SCHEDULER_SMTP_PORT,
            from_address=SCHEDULER_EMAIL_FROM,
            recipients=recipients,
            subject_prefix=SCHEDULER_EMAIL_SUBJECT_PREFIX,
            timeout_seconds=SCHEDULER_SMTP_TIMEOUT_SECONDS,
        )
        notifier.send_batch_completion(batch_info)
    except Exception:
        _logger.error(
            "Unexpected error while dispatching batch completion email",
            exc_info=True,
        )


# ─── Drain loop ──────────────────────────────────────────────────────────


def _find_free_slot() -> Optional[int]:
    """Return a free slot index, or None if all slots are busy."""
    with _slot_lock:
        for idx in range(1, SCHEDULER_PARALLEL_SLOTS + 1):
            if _active_slots.get(idx) is None:
                return idx
    return None


def _in_flight_batch_ids() -> set[str]:
    """Return the set of batch_ids currently running on any slot.

    Reads ``_active_slots`` under ``_slot_lock``.  Used by the continuous
    drain loop to filter out batches that are already in flight, since
    a status flip to ``PROCESSING`` is written from inside ``run_batch``
    and may race briefly with a re-fetch from the drain loop.
    """
    with _slot_lock:
        return {
            batch_id
            for batch_id in _active_slots.values()
            if batch_id is not None
        }


def drain_cycle(executor: ThreadPoolExecutor) -> int:
    """Run a CONTINUOUS drain pass: dispatch ready batches to free slots.

    Re-fetches NEW jobs every ``SCHEDULER_DRAIN_RECHECK_SECONDS`` (or
    immediately when a slot frees, or immediately on SIGUSR1), so a
    long-running batch in one slot never starves incoming batches.
    Returns when (a) the queue is empty AND no slots are in flight, or
    (b) shutdown is requested.

    Algorithm per iteration:
      1. Fetch and FCFS-sort NEW jobs from the data_batch table.
      2. Filter out any whose batch_id is already in flight (defensive
         against a race between the slot's status write and our fetch).
      3. While there is a free slot and a ready job, dispatch the next
         FCFS job to the lowest-numbered free slot.
      4. If futures are pending, wait up to SCHEDULER_DRAIN_RECHECK_SECONDS
         for ANY to complete (or SIGUSR1 to fire).  Drain completed
         futures, then loop.
      5. If no futures pending AND the queue was empty, return.

    Returns:
        Total number of batches dispatched across all iterations.

    Notes:
      - Honours SIGUSR1: the handler sets ``_wake_pending`` which short-
        circuits the inner wait so a 'r' keypress in ovid-watch refreshes
        the queue without waiting for the recheck interval.
      - Honours ``_shutdown_requested``: stops dispatching new batches
        but allows already-running futures to finish (cascade-stop is
        handled separately via _cascade_terminate_slots).
    """
    create_ovid_db_if_not_found()

    futures: dict[Future, dict] = {}
    submitted = 0

    while True:
        if _shutdown_requested.is_set():
            _logger.info(
                "Shutdown requested; stopping drain dispatch "
                "(in-flight slots will finish via cascade-stop)"
            )
            break

        # Step 1+2: fetch ready jobs minus those already in flight.
        try:
            fetched = fetch_new_jobs()
        except Exception:
            _logger.error(
                "fetch_new_jobs() raised; will retry after recheck",
                exc_info=True,
            )
            fetched = []

        in_flight = _in_flight_batch_ids()
        ready = [
            j for j in sort_jobs_fcfs(fetched)
            if f"{j['ovid_partner']}_{j['ovid_folder']}" not in in_flight
        ]

        if ready:
            _logger.info(
                f"Drain: {len(ready)} ready job(s) "
                f"(in-flight: {sorted(in_flight)}); "
                f"slots={SCHEDULER_PARALLEL_SLOTS}"
            )

        # Step 3: dispatch FCFS to any free slots.
        dispatched_this_iter = 0
        while ready:
            slot = _find_free_slot()
            if slot is None:
                break  # no free slot; wait for one to free up
            job = ready.pop(0)
            batch_id = f"{job['ovid_partner']}_{job['ovid_folder']}"
            _logger.info(
                f"Dispatching {batch_id} to slot {slot} "
                f"(received_time={job.get('_received_time')})"
            )
            # Claim the slot here BEFORE submitting so a subsequent
            # _find_free_slot() in this same iteration can't hand the
            # same slot to a second batch.  run_batch() overwrites the
            # same key with the actual batch_id at its top — harmless.
            with _slot_lock:
                _active_slots[slot] = batch_id
            fut = executor.submit(run_batch, slot, job)
            futures[fut] = {"slot": slot, "batch_id": batch_id, "job": job}
            submitted += 1
            dispatched_this_iter += 1

        # Step 4 / 5: nothing left to dispatch right now.  Either wait
        # for an in-flight slot to free up (timed) or exit the drain.
        if not futures:
            # Queue is empty AND no slots in flight.  Caller (the
            # orchestrator main loop) will sleep on the long poll
            # interval.
            if dispatched_this_iter == 0:
                break
            else:
                # We just dispatched but they finished synchronously?
                # Extremely unlikely (subprocess startup is non-trivial)
                # but loop again to be safe.
                continue

        # Block until ANY slot future completes or the recheck deadline
        # fires (so we can pick up newly-arrived jobs without operator
        # action).  SIGUSR1 also short-circuits this via _wake_pending.
        _wake_pending.clear()
        wait_timeout = SCHEDULER_DRAIN_RECHECK_SECONDS
        try:
            done, _pending = wait(
                list(futures.keys()),
                timeout=wait_timeout,
                return_when=FIRST_COMPLETED,
            )
        except Exception:
            _logger.error(
                "wait() on slot futures raised; treating as recheck tick",
                exc_info=True,
            )
            done = set()

        for fut in done:
            _drain_completed_future(fut, futures)

        if _wake_pending.is_set():
            _logger.info(
                "Wake-pending flag set during drain; refreshing queue now"
            )
            _wake_pending.clear()

        # Loop back to re-fetch.  If nothing changed (no slot freed and
        # no new jobs), we just paid one recheck-interval of latency
        # and the next iteration will re-fetch and find the same state.

    _logger.info(
        f"Drain cycle complete: dispatched {submitted} batch(es) "
        f"(in-flight at exit: {sorted(_in_flight_batch_ids())})"
    )
    return submitted


def _drain_completed_future(fut: Future, futures: dict[Future, dict]) -> None:
    """Pop a completed future from the tracking dict and log any exception."""
    info = futures.pop(fut, None)
    try:
        fut.result()
    except Exception:
        if info is not None:
            _logger.error(
                f"Unhandled exception while running {info.get('batch_id')} "
                f"on slot {info.get('slot')}; continuing",
                exc_info=True,
            )
        else:
            _logger.error(
                "Unhandled exception in slot future (no tracking info)",
                exc_info=True,
            )


# ─── Main entry point ─────────────────────────────────────────────────────


def run_orchestrator() -> None:
    """Top-level scheduler entry point (called from ovid.py).

    Initialises logging, wipes stale PROCESSING rows, and enters the
    drain-and-sleep loop. Runs forever; exits only on SIGINT/SIGTERM
    (handled by the systemd unit).
    """
    global _logger

    timestamp = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    _logger = setup_logging(
        timestamp=timestamp,
        partner="scheduler",
        base_dir=SCHEDULER_SLOT_LOG_DIR,
    )
    _logger.info(
        f"Snowflake connector version: {snowflake.connector.__version__}"
    )
    _logger.info(
        f"Starting OVID Scheduler (host-side orchestrator mode); "
        f"slots={SCHEDULER_PARALLEL_SLOTS}, "
        f"workers_per_slot={SCHEDULER_WORKERS_PER_SLOT}, "
        f"idle poll every {SCHEDULER_POLL_INTERVAL_IN_MINUTES} minutes"
    )

    # Initialise slot tracking dict.
    for idx in range(1, SCHEDULER_PARALLEL_SLOTS + 1):
        _active_slots[idx] = None

    # One-time startup recovery.
    try:
        create_ovid_db_if_not_found()
        reset_processing_to_error_on_startup()
    except Exception:
        _logger.error("Startup recovery failed; continuing", exc_info=True)

    # ─── SIGUSR1 wake handler ───────────────────────────────────────────
    #
    # Install a SIGUSR1 handler so an external process (notably ovid-watch
    # via its 'r' keypress) can wake the orchestrator from its poll-cycle
    # `time.sleep(SCHEDULER_POLL_INTERVAL)` immediately, without ever
    # corrupting an in-progress drain_cycle().
    #
    # The handler is dual-mode:
    #
    #   1. If ``_sleep_active[0]`` is True (the main thread is parked in
    #      ``time.sleep`` — see the wrapper around the poll-sleep call
    #      below), the handler raises ``_WakeRequested``.  PEP 475 makes
    #      this the ONLY reliable way to interrupt ``time.sleep`` in
    #      Python 3.5+: a handler that just returns lets Python re-enter
    #      ``nanosleep(2)`` for the remaining duration.  The exception
    #      unwinds through the sleep and is caught by the inner
    #      ``except _WakeRequested:`` clause.
    #
    #   2. If ``_sleep_active[0]`` is False (drain_cycle is running, or
    #      the main thread is anywhere else), the handler does NOT raise.
    #      Raising mid-drain corrupts whatever syscall is in flight —
    #      Snowflake's auth handshake re-wraps ``_WakeRequested`` as
    #      ``Failed to load private key`` errors (the 2026-06-12 cascade),
    #      ``os.listdir`` returns inconsistent state, etc.  Instead the
    #      handler sets ``_wake_pending`` (a threading.Event) and returns.
    #      The main loop checks the event at the top of every iteration
    #      and SKIPS the next sleep entirely when set, so the rescan
    #      still happens immediately — no corruption, no exception.
    #
    # Why this is safe:
    #   - Signal handlers run on the main thread in Python; both flags
    #     are read/written on the main thread only, so no lock is needed.
    #   - Slot subprocesses are spawned via ThreadPoolExecutor worker
    #     threads and don't see SIGUSR1 anyway (and additionally use
    #     ``start_new_session=True`` so they don't share signal lifecycle).
    #   - ``_wake_pending`` is edge-triggered (a threading.Event), so
    #     multiple rapid 'r' presses collapse to a single wake.
    #
    # Trigger from the host:
    #   kill -USR1 $(systemctl show ovid-scheduler --property=MainPID --value)
    #
    # Trigger from ovid-watch:
    #   Press 'r' in the live viewer.
    def _handle_wake_signal(_signum, _frame):
        # ALWAYS set the pending flag so the loop knows to skip the
        # next sleep, even if we don't end up raising right now.
        _wake_pending.set()
        if _sleep_active[0]:
            _logger.info(
                "SIGUSR1 received during poll sleep: interrupting now"
            )
            # PEP 475 escape: only safe inside time.sleep(), which is
            # the only place we set _sleep_active[0] = True.
            raise _WakeRequested()
        else:
            _logger.info(
                "SIGUSR1 received during drain; will skip next sleep "
                "(letting current scan finish first)"
            )

    try:
        signal.signal(signal.SIGUSR1, _handle_wake_signal)
        _logger.info(
            "SIGUSR1 wake handler installed "
            "(used by ovid-watch's 'r' key to trigger an immediate rescan)"
        )
    except (ValueError, OSError) as exc:
        # signal.signal() raises ValueError if called from a non-main thread
        # and OSError on platforms that don't support SIGUSR1 (Windows).
        # Both are non-fatal: the orchestrator just won't be wake-able.
        _logger.warning(
            f"Failed to install SIGUSR1 wake handler ({exc!r}); "
            f"the scheduler will only run on its 15-minute poll cycle"
        )

    # ─── Cascade-stop signal handler ───────────────────────────────────
    #
    # systemctl stop ovid-scheduler delivers SIGINT (per the unit's
    # KillSignal=SIGINT setting), which Python turns into a
    # KeyboardInterrupt on the main thread when it isn't blocked in
    # something signal-uninterruptible. The main loop's existing
    # `except KeyboardInterrupt` clause catches it and breaks out.
    #
    # Pre-2026-06-16 the slot subprocesses survived this because they
    # were launched with start_new_session=True (separate process
    # group). The 2026-06-16 cascade-stop change keeps the
    # start_new_session=True behaviour AND adds an explicit handler
    # here that, on SIGINT/SIGTERM, sets _shutdown_requested and
    # dispatches SIGTERM to each slot's process group via
    # _cascade_terminate_slots(). After that, the handler raises
    # KeyboardInterrupt so the existing main-loop break path fires.
    #
    # SIGTERM gets the same treatment as SIGINT for symmetry — an
    # operator's `kill <pid>` should behave identically to systemctl's
    # SIGINT.
    def _handle_cascade_stop(signum, _frame):
        if _shutdown_requested.is_set():
            # Already shutting down; ignore repeat signals so the
            # cascade isn't restarted while it's still in progress.
            return
        signal_name = signal.Signals(signum).name
        _logger.info(
            f"{signal_name} received; cascade-stopping all in-flight "
            f"slot subprocesses (raw_upload + email notifications "
            f"will be skipped for any batch still running)"
        )
        _shutdown_requested.set()
        try:
            n = _cascade_terminate_slots(grace_seconds=30.0)
            _logger.info(f"Cascade-stop: signalled {n} slot subprocess(es)")
        except Exception:
            _logger.error(
                "Cascade-stop helper raised; continuing shutdown anyway",
                exc_info=True,
            )
        # Raise KeyboardInterrupt so the main loop's existing break
        # path fires and the ThreadPoolExecutor `with` block exits.
        # Slot threads are now blocked in proc.wait() against
        # subprocesses we just SIGTERM'd — they will return shortly
        # and exit, freeing the executor.
        raise KeyboardInterrupt()

    try:
        signal.signal(signal.SIGTERM, _handle_cascade_stop)
        signal.signal(signal.SIGINT, _handle_cascade_stop)
        _logger.info("SIGTERM/SIGINT cascade-stop handlers installed")
    except (ValueError, OSError) as exc:
        _logger.warning(
            f"Failed to install SIGTERM/SIGINT cascade-stop handler ({exc!r}); "
            f"systemctl stop may leave orphan slot subprocesses"
        )

    with ThreadPoolExecutor(
        max_workers=SCHEDULER_PARALLEL_SLOTS,
        thread_name_prefix="ovid-slot",
    ) as executor:
        while True:
            try:
                _logger.info("Checking for new jobs...")
                processed = drain_cycle(executor)
                if processed > 0:
                    _logger.info(
                        "Cycle produced jobs; re-scanning immediately for more"
                    )
                    continue
                # If a SIGUSR1 arrived while we were inside drain_cycle
                # (rather than during the upcoming sleep), the handler
                # set _wake_pending without raising.  Honour it now by
                # skipping the sleep entirely.
                if _wake_pending.is_set():
                    _wake_pending.clear()
                    _logger.info(
                        "Wake-pending flag set during drain; "
                        "skipping sleep and rescanning immediately"
                    )
                    continue
                _logger.info(
                    f"No ready jobs; sleeping "
                    f"{SCHEDULER_POLL_INTERVAL_IN_MINUTES} minutes "
                    f"before next scan (send SIGUSR1 to wake early)..."
                )
                # Mark that the main thread is parked in time.sleep so
                # the SIGUSR1 handler knows it's safe to raise
                # _WakeRequested (PEP 475 escape).  ALWAYS clear the
                # flag in finally so a stray exception leaves the
                # handler in its safe-default no-raise mode.
                _sleep_active[0] = True
                try:
                    time.sleep(SCHEDULER_POLL_INTERVAL)
                except _WakeRequested:
                    _logger.info(
                        "Poll sleep interrupted by SIGUSR1; rescanning immediately"
                    )
                finally:
                    _sleep_active[0] = False
                # Either we woke naturally or via SIGUSR1; clear any
                # pending flag the handler set as a side effect.
                _wake_pending.clear()
            except KeyboardInterrupt:
                # Trigger cascade-stop if it hasn't already fired.
                # This covers the case of a real Ctrl+C from a
                # controlling tty (SIGINT goes to Python's default
                # handler which raises KeyboardInterrupt directly,
                # bypassing our SIGTERM cascade handler).
                if not _shutdown_requested.is_set():
                    _shutdown_requested.set()
                    _logger.info(
                        "KeyboardInterrupt received; cascade-stopping "
                        "all in-flight slot subprocesses"
                    )
                    try:
                        n = _cascade_terminate_slots(grace_seconds=30.0)
                        _logger.info(
                            f"Cascade-stop: signalled {n} slot subprocess(es)"
                        )
                    except Exception:
                        _logger.error(
                            "Cascade-stop helper raised; continuing "
                            "shutdown anyway",
                            exc_info=True,
                        )
                _logger.info("Scheduler stopped by user.")
                break
            except Exception as e:
                _logger.error(
                    f"Error in scheduler drain loop: {e}", exc_info=True
                )
                # Honour a pending wake during the error path too.
                if _wake_pending.is_set():
                    _wake_pending.clear()
                    _logger.info(
                        "Wake pending after error; retrying drain immediately"
                    )
                    continue
                # Signal-interruptible backoff sleep (same pattern as
                # the main poll-sleep above).
                _sleep_active[0] = True
                try:
                    time.sleep(SCHEDULER_POLL_INTERVAL)
                except _WakeRequested:
                    _logger.info(
                        "Error-backoff sleep interrupted by SIGUSR1; "
                        "retrying drain cycle"
                    )
                finally:
                    _sleep_active[0] = False
                _wake_pending.clear()

    _logger.info("Orchestrator main loop exited; waiting for any in-flight "
                 "slot subprocesses to finish before returning")


if __name__ == "__main__":
    run_orchestrator()
