"""OVID Tools Pipeline.

Entry point for ad-hoc utilities that run inside the Ovid Spark cluster but
sit outside the standard ETL / analysis / DQ-checks workflow.

Currently exposes one job:

    raw_upload  -- Upload one or more raw input files (pre-format) from
                   ``/data/{folder}/`` to Snowflake under
                   ``{db_name}.RAW_{folder_basename}.{TABLE_NAME}``.

The tools pipeline is invoked from the host orchestrator via::

    python3 ovid.py tools -j raw_upload -p emy -f EMY/20260319 -t all

See ``common/commonFunctions.py:db_upload_raw`` for the per-file upload
implementation, and ``common/commonFunctions.py:_raw_dv_match_files`` for
the substring/glob-based ``-t`` matching shared with the
``raw_distinct_values`` analysis job.
"""

import argparse
import importlib
import os
import re
import sys

import pyspark
from pyspark.sql import SparkSession

from commonFunctions import CommonFuncitons as cf
from settings import VALID_TOOLS_JOBS
from ovid_logging import (
    setup_logging,
    get_logger,
    log_job_start,
    log_job_end,
    print_pipeline_summary,
)


# Ensure /app/common is on PYTHONPATH for any subprocesses spawned by the
# tools pipeline. spark-submit --py-files distributes common.zip via a
# temp directory that may be cleaned up after spark.stop() returns,
# breaking imports in child processes that outlive the driver.
_pypath = os.environ.get('PYTHONPATH', '')
if '/app/common' not in _pypath:
    os.environ['PYTHONPATH'] = '/app/common' + (':' + _pypath if _pypath else '')


master_url = os.environ["SPARK_URL"].strip().replace('"', '')

# Extract timestamp from master URL (format: spark://ovid-master-YYYYMMDD-HHMMSS-<hex>:7077)
master_name = master_url.replace('spark://', '').replace(':7077', '')
timestamp = master_name.replace('ovid-master-', '')


# Build a throwaway SparkSession so spark-submit's connection setup is
# verified at startup, then immediately shut it down. The actual work is
# done by per-job sessions created inside commonFunctions.
spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()
spark.sparkContext.setLogLevel('OFF')
spark.stop()


job_num = 0


###################################################################################################################################
# Argument parsing
###################################################################################################################################

parser = argparse.ArgumentParser()

parser.add_argument("-j", "--job", nargs="+", required=True)
parser.add_argument("-p", "--partner", required=True)
parser.add_argument("-t", "--table", nargs="+", required=False)
parser.add_argument("-f", "--data_folder", nargs="+", required=True)
parser.add_argument("-db", "--database", required=False)
parser.add_argument("-s", "--db_server", required=False)
parser.add_argument("-dt", "--database_type", required=False)


args = parser.parse_args()

input_jobs         = args.job
input_partner      = args.partner.lower()
# -t is optional; default to ['all'] so the user can omit it to upload
# every csv/tsv/txt in /data/{folder}/.
input_tables       = args.table if args.table is not None else ['all']
input_data_folders = args.data_folder
input_db           = args.database
input_db_server    = args.db_server
input_db_type      = args.database_type if args.database_type is not None else 'SF'


# Print startup summary
print_pipeline_summary("OVID Tools Pipeline", [
    ("Partner",     input_partner),
    ("Folder(s)",   ', '.join(input_data_folders) if input_data_folders else 'N/A'),
    ("Tools",       ', '.join(input_jobs) if input_jobs else 'N/A'),
    ("Tables",      ', '.join(input_tables) if input_tables else 'N/A'),
    ("Database",    input_db or 'N/A'),
    ("DB Type",     input_db_type or 'N/A'),
    ("DB Server",   input_db_server or 'N/A'),
    ("Instance",    timestamp),
])
# Flush stdout immediately so the banner reaches the host-side log streamer
# even when the parent harness (opencode/ansible/CI) buffers or terminates
# stdout before the live-display thread connects.
sys.stdout.flush()

# Initialize logging with timestamp and partner
logger = setup_logging(timestamp=timestamp, partner=input_partner)
logger.info(
    f"Tools Pipeline started - Partner: {input_partner}, "
    f"Jobs: {input_jobs}, Tables: {input_tables}, "
    f"Folders: {input_data_folders}"
)

# Clear stale failure queue and flag from previous runs to prevent old
# failure messages from being displayed in this run.
cf.clear_stale_sync_files()


logger.info("=" * 60)
logger.info("Starting Tools job execution")
logger.info("=" * 60)


###################################################################################################################################
# Helpers
###################################################################################################################################

def _resolve_raw_files(raw_input_dir: str, table_patterns: list) -> list:
    """Resolve ``-t`` patterns to a sorted list of absolute raw file paths.

    For ``-t all`` every csv/tsv/txt file in ``raw_input_dir`` is returned
    (no exclusions: vocabulary tables, supplemental tables, and any other
    non-CDM helper files are all included by design).

    For explicit patterns the existing ``CommonFuncitons._raw_dv_match_files``
    helper is reused so behaviour matches the ``raw_distinct_values``
    analysis job: plain patterns are substring-matched (``-t measurement``
    matches ``measurement.csv``, ``measurement_sup.csv``, and
    ``cerner_measurement.csv``); patterns containing glob meta-characters
    (``*``, ``?``, ``[``) are passed through fnmatch verbatim.

    Args:
        raw_input_dir: Absolute path to the raw data directory mounted at
            ``/data/{folder}/`` inside the Spark master container.
        table_patterns: List of ``-t`` patterns or the literal ``'all'``.

    Returns:
        Sorted list of absolute file paths. Empty list when no files
        match (caller is responsible for warning).
    """
    if not os.path.isdir(raw_input_dir):
        logger.error(f"Raw input directory does not exist: {raw_input_dir}")
        return []

    # 'all' for the tools.raw_upload job means EVERY raw csv/tsv/txt --
    # no exclusions. This deliberately diverges from raw_distinct_values
    # which skips OMOP reference vocabulary tables; for raw upload the
    # user explicitly asked that 'all' include every file.
    run_all = any(p.lower() == 'all' for p in table_patterns)
    if run_all:
        from commonFunctions import RAW_DV_ALLOWED_EXTENSIONS
        matched = []
        for entry in sorted(os.listdir(raw_input_dir)):
            full_path = os.path.join(raw_input_dir, entry)
            if not os.path.isfile(full_path):
                continue
            ext = os.path.splitext(entry)[1].lower()
            if ext not in RAW_DV_ALLOWED_EXTENSIONS:
                continue
            matched.append(full_path)
        return matched

    # Explicit patterns: defer to the shared substring/glob helper.
    return cf._raw_dv_match_files(raw_input_dir, table_patterns)


def _file_to_table_name(file_path: str) -> str:
    """Derive a Snowflake-safe table name from a raw file path.

    The table name is the input filename, uppercased, with the file
    extension stripped, then sanitized to satisfy Snowflake's unquoted
    identifier rules so the generated SQL never breaks on reserved
    characters (parentheses, spaces, hyphens, etc.). The sanitization
    rule is:

      1. Strip the extension and uppercase the stem.
      2. Replace every character that is not ``[A-Z0-9_]`` with ``_``.
      3. Collapse runs of ``_`` to a single ``_``.
      4. Trim leading and trailing ``_``.
      5. If the result starts with a digit, prefix ``T_`` so the
         identifier remains valid (Snowflake unquoted identifiers must
         start with a letter or underscore).

    For well-named files (alphanumeric + underscore only) the rule is a
    no-op, preserving the data lineage from raw delivery to Snowflake
    table that motivates the verbatim-filename policy. For filenames
    containing reserved characters (e.g. ``OneFlorida_DatavantLinkage_(20251001_20260228).txt``)
    the rule produces a queryable identifier (``ONEFLORIDA_DATAVANTLINKAGE_20251001_20260228``).
    Whenever the sanitizer changes the name, an INFO log line records
    both forms so the operator can see in the OVID log how the table was
    renamed.

    Args:
        file_path: Absolute path to the raw file.

    Returns:
        Snowflake-safe uppercased identifier derived from the filename
        stem.
    """
    base = os.path.basename(file_path)
    stem, _ = os.path.splitext(base)
    raw = stem.upper()
    # Replace every non-[A-Z0-9_] character with _, collapse runs, trim.
    sanitized = re.sub(r'[^A-Z0-9_]', '_', raw)
    sanitized = re.sub(r'_+', '_', sanitized).strip('_')
    if not sanitized:
        # Pathological case: filename was entirely reserved characters.
        sanitized = 'T_UNNAMED'
    elif sanitized[0].isdigit():
        sanitized = 'T_' + sanitized
    if sanitized != raw:
        logger.info(
            f"[raw_upload] sanitized table name '{raw}' -> '{sanitized}' "
            f"(source file: {base})"
        )
    return sanitized


###################################################################################################################################
# raw_upload job
###################################################################################################################################

def run_raw_upload_jobs(folder: str) -> None:
    """Upload raw input files for ``folder`` to Snowflake sequentially.

    Resolves the partner's database name (when ``-db`` was not supplied)
    from ``partners/{partner}/partner_settings.py`` so the user does not
    need to type it on every invocation. Files are uploaded one at a
    time; the per-file Spark write is internally parallelised by the
    cluster, so host-side parallelism would not help.

    Args:
        folder: Folder identifier (e.g. ``EMY/20260319``). Maps to the
            ``/data/{folder}/`` mount inside the Spark master container.
    """
    global job_num

    raw_input_dir = f'/data/{folder}/'

    matched_files = _resolve_raw_files(raw_input_dir, input_tables)

    if not matched_files:
        logger.warning(
            f"[raw_upload] no files in {raw_input_dir} matched table patterns {input_tables}"
        )
        return

    logger.info(
        f"[raw_upload] matched {len(matched_files)} file(s) under {raw_input_dir}"
    )

    # Resolve the target database name. Prefer the explicit -db argument;
    # fall back to the partner's default. Raising clearly here is better
    # than letting Snowflake reject a None database name later.
    db_name = input_db
    if db_name is None:
        try:
            partner_settings_path = "partners." + input_partner + ".partner_settings"
            partner_settings = importlib.import_module(partner_settings_path)
            db_name = getattr(partner_settings, 'DEFAULT_PARTNER_DB_NAME', None)
        except ImportError as exc:
            logger.error(
                f"[raw_upload] could not import partner_settings for {input_partner}: {exc}",
                exc_info=True,
            )
            db_name = None

    if not db_name:
        logger.error(
            f"[raw_upload] no database name supplied via -db and "
            f"partners/{input_partner}/partner_settings.py:DEFAULT_PARTNER_DB_NAME "
            f"is missing or empty. Aborting."
        )
        return

    folder_base = folder.split('/')[-1] if '/' in folder else folder
    schema_name = f"RAW_{folder_base.upper()}"

    for file_path in matched_files:
        table_name = _file_to_table_name(file_path)

        job_num = job_num + 1
        cf.print_run_status(
            job_num,
            total_jobs_count,
            f"raw uploading to {db_name.upper()}.{schema_name}.{table_name}",
            folder,
            input_partner,
        )

        try:
            cf.db_upload_raw(
                db=db_name,
                db_server=input_db_server,
                file_path=file_path,
                table_name=table_name,
                folder_name=folder,
                db_type=input_db_type,
                partner_name=input_partner,
            )
        except Exception as exc:
            logger.error(
                f"[raw_upload] failed to upload {file_path} -> "
                f"{db_name}.{schema_name}.{table_name}: {exc}",
                exc_info=True,
            )
            cf.print_failure_message(
                folder=folder,
                partner=input_partner,
                job=f"raw_upload {table_name}",
                text=str(exc),
            )


###################################################################################################################################
# Input validation
###################################################################################################################################

if not cf.valid_partner_name(input_partner):
    logger.error(f"Unrecognized partner: {input_partner}")
    print("Error: Unrecognized partner " + input_partner + " !!!!!")
    sys.exit()


for job in input_jobs:
    if job not in VALID_TOOLS_JOBS:
        logger.error(f"Invalid tools job specified: {job}")
        print(
            f"{job} is not a valid tools job. "
            f"Valid jobs: {', '.join(VALID_TOOLS_JOBS)}"
        )
        sys.exit()


# raw_upload requires Snowflake credentials; surface missing -s early so
# we don't tear down the cluster after the failed Snowflake handshake.
if 'raw_upload' in input_jobs and not input_db_server:
    logger.error(
        "[raw_upload] -s/--db_server is required (Snowflake account locator)."
    )
    print("Error: -s/--db_server is required for raw_upload (Snowflake account locator).")
    sys.exit()


# Verify every requested folder exists before doing any work.
for folder in input_data_folders:
    path = f"/data/{folder}"
    if not os.path.exists(path):
        logger.error(f"Path does not exist: {path}")
        print("Path  " + path + " does not exits !!!!!!!")
        sys.exit()


###################################################################################################################################
# Total job count for the live status display
###################################################################################################################################

global total_jobs_count

folders_count = len(input_data_folders)
jobs_count    = len(input_jobs)

# We don't know the matched-file count until we walk each folder, so
# overestimate using the user's -t list (or a reasonable default for
# 'all') so the live status counter has a sane upper bound. The display
# tolerates job_num exceeding total_jobs_count.
if 'all' in [t.lower() for t in input_tables]:
    # Best-effort: count files in the first folder. If multiple folders
    # are passed, we just multiply by folders_count.
    try:
        first_path = f"/data/{input_data_folders[0]}"
        from commonFunctions import RAW_DV_ALLOWED_EXTENSIONS
        tables_count = sum(
            1 for entry in os.listdir(first_path)
            if os.path.isfile(os.path.join(first_path, entry))
            and os.path.splitext(entry)[1].lower() in RAW_DV_ALLOWED_EXTENSIONS
        ) or 1
    except Exception:
        tables_count = 1
else:
    tables_count = len(input_tables)

total_jobs_count = folders_count * jobs_count * tables_count


###################################################################################################################################
# Dispatch
###################################################################################################################################

JOB_DISPATCH = {
    'raw_upload': run_raw_upload_jobs,
}


for folder in input_data_folders:
    logger.info(f"Processing folder: {folder}")

    print("""
      

                            ▄█  ░█▀▀▀ ░█       ░█▀▀█ ░█ ░█  █▀▀█ ░█▄ ░█ ░█▀▀█ ░█▀▀▀ ▀█▀ ░█▄ ░█  █▀▀█ ▀▀█▀▀ ░█▀▀▀█ ░█▀▀█
                             █  ░█▀▀▀ ░█    ▀▀ ░█    ░█▀▀█ ░█▄▄█ ░█░█░█ ░█ ▄▄ ░█▀▀▀ ░█  ░█░█░█ ░█▄▄█  ░█   ░█  ░█ ░█▄▄▀
                            ▄█▄ ░█    ░█▄▄█    ░█▄▄█ ░█ ░█ ░█ ░█ ░█  ▀█ ░█▄▄█ ░█▄▄▄ ▄█▄ ░█  ▀█ ░█ ░█  ░█   ░█▄▄▄█ ░█ ░█
  
      
    """)

    for job in input_jobs:
        runner = JOB_DISPATCH.get(job)
        if runner is None:
            logger.error(f"No dispatch handler registered for tools job '{job}'")
            continue
        logger.info(f"Starting tools job: {job}")
        runner(folder)


print("""
      

                                                    ▀▀█▀▀ ░█ ░█ ░█▀▀▀ 　 ░█▀▀▀ ░█▄ ░█ ░█▀▀▄ 　 █ 
                                                     ░█   ░█▀▀█ ░█▀▀▀ 　 ░█▀▀▀ ░█░█░█ ░█ ░█ 　 ▀ 
                                                     ░█   ░█ ░█ ░█▄▄▄ 　 ░█▄▄▄ ░█  ▀█ ░█▄▄▀ 　 ▄
            
    
      """)
