POOL_SIZE      = 10
MAX_OVERFLOW   = 5
POOL_RECYCLE   = 3600
BCP_DRIVER     = 'mssql'


DEFAULT_WORKERS_COUNT = 25

DEFAULT_MASTER_MEMORY = '40g'
DEFAULT_WORKER_MEMORY = '16g'

DEFAULT_DRIVER_MEMORY            = '32g'   # Spark driver memory for the spark-submit pipeline process
DEFAULT_DRIVER_MEMORY_OVERHEAD   = '2g'    # Spark driver memory overhead for the pipeline process
DEFAULT_EXECUTOR_MEMORY_OVERHEAD = '4g'    # Spark executor memory overhead for worker tasks
DEFAULT_SUBPROCESS_DRIVER_MEMORY = '4g'    # Spark driver memory for fix/formatter/mapper subprocesses

###################################################################################################################################
# Spark SQL / serialization tuning applied to every subprocess SparkSession
# (formatter / deduplicator / mapper / fixer / upload) built by
# CommonFuncitons.get_spark_session(). Previously these lived only on the
# master-level spark-submit in ovid.py, so the child sessions that do the real
# per-table work ran with AQE off, the default 200 shuffle partitions, and Java
# serialization. Enabling AQE + coalesce lets Spark shrink the high base
# partition count down to what each table actually needs.
###################################################################################################################################

# High base shuffle-partition count; AQE coalesces this down at runtime so large
# tables get parallelism without small tables paying scheduling overhead.
DEFAULT_SHUFFLE_PARTITIONS = 600

# Kryo serializer is faster and more compact than the default Java serializer
# for shuffles and broadcasts.
DEFAULT_SPARK_SERIALIZER = 'org.apache.spark.serializer.KryoSerializer'

# Auto-broadcast threshold for dimension joins (bytes). 64 MB.
DEFAULT_AUTO_BROADCAST_JOIN_THRESHOLD = str(64 * 1024 * 1024)

DEFAULT_INPUT_DIRECTORY = ''
DEFAULT_JOBS            = 'all'
DEFAULT_TABLES          = 'all'
DEFAULT_DB_TYPE         = 'sf'
DEFAULT_DB_SERVER       = ''
###################################################################################################################################
###################################################################################################################################

AWK_PASSBY_TRESHHOLD = 40000000 # Tables more than 40M rows will not run on awk deduplicte

###################################################################################################################################
###################################################################################################################################

# Chunked parallel Datavant token-transform tuning (hash_token formatting).
# The parallel transform splits the input file into ~HASH_TOKEN_CHUNK_SIZE_TARGET
# byte chunks and runs the Datavant CLI on each chunk concurrently. The chunk
# count is ceil(file_size / target), clamped to [1, HASH_TOKEN_MAX_CHUNKS].
#
# HASH_TOKEN_MAX_CHUNKS caps per-partner concurrency. Worst case across the
# scheduler is SCHEDULER_PARALLEL_SLOTS * HASH_TOKEN_MAX_CHUNKS concurrent
# Datavant subprocesses (3 * 16 = 48 on the 96-core host), leaving headroom
# for the per-slot Spark clusters and disk I/O.
HASH_TOKEN_CHUNK_SIZE_TARGET = 50 * 1024 * 1024  # 50 MB target per Datavant chunk
HASH_TOKEN_MAX_CHUNKS        = 16

###################################################################################################################################
###################################################################################################################################
VALID_ETL_JOBS = [  
                    'all',
                    'map',
                    'format',
                    'deduplicate',
                    'upload', 
                    'fix',
                    ]

VALID_ANALYSIS_JOBS = [  
                    'all',
                    'format',
                     'monthly_counts',
                    'distinct_values',
                    'raw_distinct_values',
                   ]


VALID_DQ_CHECKS_JOBS = [  
                    'all',
                    'format',
                    'mapping_gap',
                    'mapping_report',
                    'dc_checks',
                   ]

VALID_TOOLS_JOBS = [
                    'raw_upload',
                   ]


###################################################################################################################################
###################################################################################################################################



HARVEST_CDM_VERSION     = '070'
MGMT_FIELDS_VALUE       = '01'


###################################################################################################################################
###################################################################################################################################



###################################################################################################################################
###################################################################################################################################

harmonized_filed_sizes_dict = {

                            "PATID"               :"64",
                            "PATID_1"             :"64",
                            "PATID_2"             :"64",
                            "PERSON_ID"           :"64",
                            "ENCOUNTERID"         :"128",
                            "PROVIDERID"          :"64",
                            "VX_PROVIDERID"       :"64",
                            "MEDADMIN_PROVIDERID" :"64",
                            "OBSGEN_PROVIDERID"   :"64",
                            "OBSCLIN_PROVIDERID"  :"64",
                            "RX_PROVIDERID"       :"64",
                            "PROCEDURESID"        :"128",
                            "PRESCRIBINGID"       :"128",
                            "ADDRESSID"           :"64",
                            "GEOCODEID"           :"64",
                            
                            }

###################################################################################################################################
########################    Scheduler settings ####################################################################################
###################################################################################################################################

# SCHEDULER_MODE selects between two architectures:
#   'orchestrator' (default, since 2026-06-12) -- the scheduler is a
#       host-side Python process that maintains N parallel "slots".
#       Each slot spawns its own `ovid etl` subprocess (which in turn
#       creates its own per-instance Spark cluster via the existing
#       ovid.py instance isolation), runs one batch end-to-end, and
#       writes its raw ANSI live-display stream to a per-slot log file
#       (systemd-scheduler-slot-<N>.log). `ovid-watch` shows one page
#       per slot. There is NO long-lived scheduler-owned master
#       container in this mode -- the scheduler process itself runs
#       on the host with no Spark dependency.
#   'legacy' -- the pre-2026-06-12 architecture: scheduler runs inside
#       a long-lived master container (spawned by ovid.py at startup),
#       processes batches sequentially via in-container `docker exec`
#       + spark-submit. Kept available for emergency rollback. Set
#       this value if the new orchestrator misbehaves and you need
#       to revert without redeploying code.
SCHEDULER_MODE = 'orchestrator'

# Number of partner batches the orchestrator runs in parallel.
# Each slot owns its own Spark cluster (master + workers) for the
# duration of one batch. Total host memory reserved is approximately
#   SCHEDULER_PARALLEL_SLOTS * (DEFAULT_MASTER_MEMORY
#                               + SCHEDULER_WORKERS_PER_SLOT * DEFAULT_WORKER_MEMORY)
# and this product must fit within the host's Docker memory budget with
# headroom for the OS and page cache. Tune these two values to your host's
# available memory; launching many large worker clusters within a short
# window can starve a slot's Spark master ("All masters are unresponsive").
SCHEDULER_PARALLEL_SLOTS = 3
SCHEDULER_WORKERS_PER_SLOT = 25

# Directory where slot log files are written. One file per slot:
#   systemd-scheduler-slot-1.log
#   systemd-scheduler-slot-2.log
# Files are truncated when a new batch begins on that slot, so
# the live viewer always shows the currently-running batch's frames.
SCHEDULER_SLOT_LOG_DIR = 'work_directory/docker_data/ovid_logs'

SCHEDULER_MAX_THREADS= 1  # legacy, kept for back-compat; unused by orchestrator mode
SCHEDULER_POLL_INTERVAL_IN_MINUTES = 15
SCHEDULER_POLL_INTERVAL = SCHEDULER_POLL_INTERVAL_IN_MINUTES * 60
# How often (in seconds) the continuous drain loop re-fetches NEW jobs
# while batches are already in flight.  Lower = faster reaction to new
# deliveries; higher = fewer Snowflake metadata queries.  Default 30s.
# (Since 2026-06-25, the drain loop no longer waits for all in-flight
#  batches to finish before re-fetching; it polls at this cadence so a
#  long-running batch in one slot never starves incoming batches.)
SCHEDULER_DRAIN_RECHECK_SECONDS = 30
SCHEDULER_DB_TYPE   = 'sf'
SCHEDULER_DB_SERVER = ''
SCHEDULER_DB_NAME   = 'OVID'
SCHEDULER_DB_SCHEMA    = 'SCHEDULER'

# Fallback quiet-period gate (in minutes). Historically this was the ONLY
# gate: a batch folder had to be "quiet" (no ctime change) for this long
# before the scheduler would dispatch it, as a heuristic guess that the
# archive process had finished writing. Since the event-driven ready-marker
# signal (2026-09-10) this is now only a FALLBACK: a batch becomes eligible
# immediately when a ready-marker file is present (see
# SCHEDULER_READY_MARKER_FILENAMES), and this quiet-period only applies to
# folders that never receive a marker (partners/archives that have not yet
# adopted signaling). Kept so no partner's data stalls forever.
MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES = 10  #  minutes

# Event-driven "batch ready" signal (since 2026-09-10).
#
# Instead of guessing that an archive has finished writing by waiting
# MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES of on-disk quiet, the
# archive/upload process can explicitly signal readiness by dropping a
# marker file into the batch folder (/data/{partner}/{folder}/) as the
# LAST step after all data files are written. When the scheduler sees any
# of these filenames it dispatches the batch immediately (subject to the
# usual eligibility gate) and acknowledges by transitioning the
# data_batch row NEW -> READY and stamping signal_received_time.
#
# The marker filename(s) are intentionally dot-prefixed / non-data
# extensions so the ETL and raw_upload stages (which only consume
# .csv/.tsv/.txt) never treat the marker itself as input.
#
# To force an immediate rescan instead of waiting for the poll cycle, the
# archive can additionally send SIGUSR1 to the daemon:
#   kill -USR1 $(systemctl show ovid-scheduler --property=MainPID --value)
# (or an operator can press 'r' in ovid-watch).
SCHEDULER_READY_MARKER_ENABLED = True
SCHEDULER_READY_MARKER_FILENAMES = ('.ovid_ready', '_COMPLETE')

# When True, the scheduler runs a raw_upload stage after each batch's ETL
# pipeline finishes (whether ETL succeeded or failed). Every csv/tsv/txt
# under /data/{partner}/{folder}/ is uploaded to
# {char_partner}.RAW_{folder}.{TABLE_NAME} in Snowflake. raw_upload
# failures are logged + emailed but never flip the batch status from
# PROCESSED to ERROR -- the ETL outcome is the single source of truth
# for batch status.
#
# Override per-run via:
#   1. CLI:  ovid scheduler --raw-upload-disabled
#   2. Env:  OVID_RAW_UPLOAD_DISABLED=1
#   3. The default below.
SCHEDULER_RAW_UPLOAD_ENABLED = True

###################################################################################################################################
########################    Scheduler email notifications ##########################################################################
###################################################################################################################################
#
# Recipient + enabled-flag precedence (highest first):
#   1. CLI:  ovid scheduler --email-to you@example.com  /  --email-disabled
#   2. Env:  OVID_EMAIL_RECIPIENTS="a@x,b@y"       /  OVID_EMAIL_DISABLED=1
#   3. The defaults below.
# CLI flags export the env vars; pipelines/scheduler.py reads them via
# _resolve_email_recipients() / _resolve_email_enabled().

###################################################################################################################################
########################    Datavant match-ID export inbox    #####################################################################
###################################################################################################################################
#
# Directory (inside the container) where hash_token formatters drop a timestamped
# copy of the intermediate Datavant "to datavant" file. These files are consumed
# downstream to generate Datavant match IDs.
#
# IMPORTANT: The operator is responsible for bind-mounting this container path
# to a host directory (configured via DATAVANT_MATCH_HOST_DIR below) in the
# docker compose / docker run spec so the files persist for downstream use.
# The container-spec volume blocks in ovid.py wire this mount automatically.
#
# The token transform is a two-step Datavant CLI call:
#   1) transform-tokens --from <DATAVANT_KEY> -s <site> ...
#   2) transform-tokens --to datavant         -s <site> ...
# Postprocess appends a tab-delimited 'source' column (the partner code).

# Host-side directory bind-mounted into the container at DATAVANT_MATCH_INBOX_DIR.
# Set this to a persistent path on your host. Left empty by default; configure
# it before running hash_token/Datavant export workflows.
DATAVANT_MATCH_HOST_DIR     = ''
DATAVANT_MATCH_INBOX_DIR    = '/app/to_datavant_inbox'
DATAVANT_MATCH_NUM_THREADS  = 8

# Datavant "site" identifier passed to the Datavant CLI via the `-s` argument
# during token transforms. This is organization-specific; set it to the site
# slug provided to you by Datavant for your data-sharing agreement.
DATAVANT_SITE               = ''

# Canonical value written to the pass-through TOKEN_ENCRYPTION_KEY column of
# every partner's Datavant match-export file. The Datavant transform-tokens
# CLI passes this column through untouched; normalizing to a single literal at
# postprocess time keeps the downstream match-ID pipeline invariant across all
# partners. Derived from DATAVANT_SITE by default; override if your downstream
# pipeline expects a different literal.
DATAVANT_TOKEN_ENCRYPTION_KEY_VALUE = f"datavant-{DATAVANT_SITE}_TOKEN_ENCRYPTION_KEY" if DATAVANT_SITE else ''

# Optional per-partner override for the appended ``source_site_name`` column of
# a Datavant match-export file. Map a partner's DATAVANT_KEY (substring match)
# to the literal that should be written into an extra ``source_site_name``
# column. Partners not present here get no extra column. Left empty in the
# public distribution. Example: {"my_partner_key": "my_partner_site_name"}.
DATAVANT_SOURCE_SITE_NAME_OVERRIDES = {}

SCHEDULER_EMAIL_ENABLED         = False
SCHEDULER_EMAIL_RECIPIENTS      = []

SCHEDULER_EMAIL_FROM            = ''
SCHEDULER_EMAIL_SUBJECT_PREFIX  = '[OVID Scheduler]'
SCHEDULER_SMTP_HOST             = ''
SCHEDULER_SMTP_PORT             = 25
SCHEDULER_SMTP_TIMEOUT_SECONDS  = 30
# Max ERROR/FAILURE lines pulled from the OVID log file into the email body
SCHEDULER_EMAIL_MAX_ERROR_LINES = 50

CREATE_BATCH_TABLE_DLL = f"""

                    CREATE TABLE {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch (
                                batch_id VARCHAR(50) PRIMARY KEY,
                                path TEXT NOT NULL,
                                received_time TIMESTAMP,
                                status VARCHAR(20) DEFAULT 'NEW',
                                priority VARCHAR(10),
                                processing_start TIMESTAMP,
                                processing_end TIMESTAMP,
                                signal_received_time TIMESTAMP
                    );
                    """
# NOTE: signal_received_time was added 2026-09-10 for the event-driven
# ready-marker signal. Existing OVID.SCHEDULER.data_batch tables that
# predate this need a one-time manual migration (Snowflake ignores the
# CREATE TABLE above once the table exists):
#   ALTER TABLE OVID.SCHEDULER.data_batch
#       ADD COLUMN IF NOT EXISTS signal_received_time TIMESTAMP;
# The 'READY' status value is a new intermediate state
# (NEW -> READY -> PROCESSING -> PROCESSED/ERROR); no schema change is
# needed for it because status is a free-form VARCHAR(20).


CREATE_BATCH_TABLE_FILE_DLL = f"""

                    CREATE TABLE {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch_file (
                                id BIGINT AUTOINCREMENT PRIMARY KEY,
                                batch_id VARCHAR(50) REFERENCES data_batch(batch_id) ON DELETE CASCADE,
                                file_name VARCHAR(255) NOT NULL,
                                file_size BIGINT,
                                file_hash VARCHAR(64)
                                );
                    """




###################################################################################################################################
########################    OMOP_VOCABULARY TABLES NAMES ####################################################################################
###################################################################################################################################



OMOP_VOCABULARY_TABLES_NAMES = [

                                'CONCEPT',
                                'CONCEPT_RELATIONSHIP',
                                'CONCEPT_ANCESTOR',
                                'CONCEPT_CLASS',
                                'CONCEPT_SYNONYM',
                                'DOMAIN',
                                'DRUG_STRENGTH',
                                'RELATIONSHIP',
                                'VOCABULARY',
                                'SOURCE_TO_CONCEPT_MAP',
                                ]




###################################################################################################################################
########################  PCORNET TABLES PRIMARY KEYS         ####################################################################################
###################################################################################################################################


PCORNET_PRIMARY_KEYS = {


                "DEMOGRAPHIC"        :["PATID"],
                "ENROLLMENT"         :["PATID","ENR_START_DATE","ENR_BASIS"],
                "ENCOUNTER"          :["ENCOUNTERID"],
                "DIAGNOSIS"          :["DIAGNOSISID"],
                "PROCEDURES"         :["PROCEDURESID"],
                "VITAL"              :["VITALID"],
                "DISPENSING"         :["DISPENSINGID"],
                "LAB_RESULT_CM"      :["LAB_RESULT_CM_ID"],
                "CONDITION"          :["CONDITIONID"],
                "PRO_CM"             :["PRO_CM_ID"],
                "PRESCRIBING"        :["PRESCRIBINGID"],
                "PCORNET_TRIAL"      :["PATID","TRIALID","PARTICIPANTID"],
                "DEATH"              :["PATID","DEATH_SOURCE"],
                "DEATH_CAUSE"        :["PATID","DEATH_CAUSE","DEATH_CAUSE_CODE","DEATH_CAUSE_TYPE","DEATH_CAUSE_SOURCE"],
                "MED_ADMIN"          :["MEDADMINID"],
                "PROVIDER"           :["PROVIDERID"],
                "OBS_CLIN"           :["OBSCLINID"],
                "OBS_GEN"            :["OBSGENID"],
                "HASH_TOKEN"         :["PATID","TOKEN_ENCRYPTION_KEY"],
                "LDS_ADDRESS_HISTORY":["ADDRESSID"],
                "IMMUNIZATION"       :["IMMUNIZATIONID"],
                "PAT_RELATIONSHIP"   :["PATID_1",'PATID_2','RELATIONSHIP_TYPE'],
                "EXTERNAL_MEDS"      :["EXTMEDID"],
                # Added 2026-06-12 to close the merge-upload `'NoneType' object
                # is not iterable` crash that surfaced in the EMY 20260610
                # batch (instance 20260611-132103-60d85e) at
                # commonFunctions.py:5062 when uploading PRIVATE_ADDRESS_HISTORY.
                # Keys verified against the PCORnet CDM v7.0 RELATIONAL sheet
                # (HARVEST: composite NETWORKID+DATAMARTID; LAB_HISTORY:
                # LABHISTORYID) and against mapping_scripts/pcornet_mappers/
                # tables_specs.csv for the Ovid-extension tables
                # (PRIVATE_ADDRESS_HISTORY: ADDRESSID; PRIVATE_ADDRESS_GEOCODE:
                # GEOCODEID, mirroring LDS_ADDRESS_HISTORY's ADDRESSID PK).
                "HARVEST"            :["NETWORKID","DATAMARTID"],
                "LAB_HISTORY"        :["LABHISTORYID"],
                "PRIVATE_ADDRESS_HISTORY":["ADDRESSID"],
                "PRIVATE_ADDRESS_GEOCODE":["GEOCODEID"],


}