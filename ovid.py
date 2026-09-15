
import subprocess
import signal
import sys
from common.settings import *
import time
import os
import re
import shutil
import glob
import argparse
import yaml
from common.ovid_secrets import CREDENTIALS_PATH
from common.ovid_logging import (
    setup_logging, get_logger, log_job_start, log_job_end,
    run_and_log_subprocess, acquire_terminal_lock, release_terminal_lock,
    get_log_file_path,
)
import importlib
import uuid


from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d-%H%M%S") + "-" + uuid.uuid4().hex[:6]

master_name = f'ovid-master-{timestamp}'

# Project name for docker-compose (used for consistent naming across start/stop operations)
project_name = f'ovid-{timestamp}'

# Instance-scoped file paths (prevent concurrent instance interference)
compose_file = f'work_directory/docker-compose-{timestamp}.yml'
compose_file_master = f'work_directory/docker-compose-master-{timestamp}.yml'
compose_file_workers = f'work_directory/docker-compose-workers-{timestamp}.yml'
spark_tmp_dir = f'work_directory/docker_data/spark_tmp-{timestamp}'
signal_dir = f'work_directory/docker_data/signals-{timestamp}'
common_zip_name = f'common-{timestamp}.zip'


###################################################################################################################################
# 
###################################################################################################################################
def run_on_cluster (command, folders=None, pipeline_name="ETL"):
    logger = get_logger()
    compose_args = ['docker', 'compose', '-f', compose_file, '-p', project_name]
    run_cluster_cmd       = compose_args + ['up', '-d']
    shutdown_cluster_cmd  = compose_args + ['down', '--timeout', '30', '--remove-orphans']

    def _shutdown_cluster():
        """Shut down the cluster, retrying with extended timeout on failure."""
        logger.info("Shutting down Spark cluster...")
        result = subprocess.run(" ".join(shutdown_cluster_cmd), shell=True)
        if result.returncode != 0:
            logger.warning("Initial shutdown had errors, retrying with extended timeout...")
            force_cmd = compose_args + ['down', '--timeout', '60', '--remove-orphans', '--volumes']
            subprocess.run(" ".join(force_cmd), shell=True)
        logger.info("Cluster shutdown complete.")
        # Clean up instance-scoped files
        try:
            os.remove(compose_file)
        except OSError:
            pass
        shutil.rmtree(spark_tmp_dir, ignore_errors=True)
        # Release the terminal lock so the next instance on this tty can use it
        release_terminal_lock()

    def _signal_handler(signum, frame):
        """Handle SIGTERM/SIGINT by shutting down the cluster."""
        sig_name = signal.Signals(signum).name
        logger.warning(f"Received {sig_name}, shutting down cluster...")
        _shutdown_cluster()
        raise SystemExit(1)

    # Register signal handlers so containers are cleaned up on unexpected termination
    prev_sigterm = signal.signal(signal.SIGTERM, _signal_handler)
    prev_sigint = signal.signal(signal.SIGINT, _signal_handler)

    try:
        logger.info("Starting Spark cluster...")
        subprocess.run(" ".join(run_cluster_cmd), shell=True)
        logger.info("Executing command on cluster...")
        # Capture subprocess stdout/stderr to both the terminal and the OVID log file.
        # ANSI escape codes are stripped before writing to the log.
        run_and_log_subprocess(command, shell=True, label=pipeline_name)
        if folders:
            for folder in folders:
                logger.info(f"Completed processing folder: {folder}")
        logger.info("=" * 60)
        logger.info(f"{pipeline_name} Pipeline completed successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"Error during cluster execution: {e}", exc_info=True)
    finally:
        _shutdown_cluster()
        # Restore previous signal handlers
        signal.signal(signal.SIGTERM, prev_sigterm)
        signal.signal(signal.SIGINT, prev_sigint)


###################################################################################################################################
# Stale Instance Cleanup
###################################################################################################################################

# Instance ID format produced by the timestamp at module load:
#   YYYYMMDD-HHMMSS-<6-hex>  (e.g. 20260608-162048-5eac57)
# This regex extracts that ID from any path/filename that embeds it.
_INSTANCE_ID_RE = re.compile(r'(\d{8}-\d{6}-[0-9a-f]{6})')


def _extract_instance_id(path: str) -> str | None:
    """Extract the Ovid instance ID from a path or filename.

    Args:
        path: Filesystem path or basename that may contain an embedded
            instance ID of the form ``YYYYMMDD-HHMMSS-<6-hex>``.

    Returns:
        The instance ID string if one is found, otherwise None.
    """
    match = _INSTANCE_ID_RE.search(path)
    return match.group(1) if match else None


def _instance_has_live_containers(instance_id: str) -> bool:
    """Return True if any Docker container belonging to *instance_id* is running.

    Ovid names every container ``ovid-master-{instance_id}`` or
    ``ovid-worker-{N}-{instance_id}``.  A single ``docker ps`` filtered by
    the instance suffix is sufficient to detect either.  If the docker CLI
    is unreachable or returns an error we conservatively report True (i.e.
    "do not clean") so a transient docker hiccup never destroys files for a
    live instance.

    Args:
        instance_id: The instance ID extracted from a compose file or
            instance-scoped directory name.

    Returns:
        True when at least one container with that instance ID is running,
        or when the docker liveness probe fails for any reason.  False only
        when docker responds successfully and reports no matching container.
    """
    if not instance_id:
        return False
    try:
        result = subprocess.run(
            [
                'docker', 'ps',
                '--filter', f'name={instance_id}',
                '--format', '{{.Names}}',
            ],
            capture_output=True, text=True, timeout=15,
        )
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        # Docker CLI missing or unresponsive: err on the side of preservation.
        return True
    if result.returncode != 0:
        # Non-zero exit (e.g. docker daemon down): also preserve.
        return True
    return bool(result.stdout.strip())


def cleanup_stale_instances(max_age_hours: int = 6) -> None:
    """Remove instance-scoped files left behind by crashed Ovid processes.

    Scans ``work_directory/`` for orphaned Docker Compose files, Spark temp
    directories, and signal directories whose modification time exceeds
    *max_age_hours*.  For each stale compose file an attempt is made to tear
    down any associated Docker containers before the file is deleted.

    Files belonging to instances whose Docker containers are still running
    are preserved regardless of mtime.  This protects long-lived daemons
    (notably the systemd-managed scheduler, which can run for days or
    weeks) whose compose files are written once at startup and would
    otherwise look "stale" by mtime to any concurrent ``ovid`` invocation.

    Args:
        max_age_hours: Files/directories older than this many hours are
            considered stale and will be removed (subject to the live-
            instance protection above).
    """
    logger = get_logger()
    now = time.time()
    max_age_seconds = max_age_hours * 3600

    # Stale compose files (instance-scoped names contain a timestamp)
    for pattern in [
        'work_directory/docker-compose-*.yml',
    ]:
        for filepath in glob.glob(pattern):
            if now - os.path.getmtime(filepath) > max_age_seconds:
                instance_id = _extract_instance_id(filepath)
                if instance_id and _instance_has_live_containers(instance_id):
                    logger.info(
                        f"Skipping live instance file: {filepath} "
                        f"(container for {instance_id} still running)"
                    )
                    continue
                logger.warning(f"Cleaning stale instance file: {filepath}")
                # Try to tear down any orphaned containers described in this file
                try:
                    subprocess.run(
                        ['docker', 'compose', '-f', filepath, 'down', '--remove-orphans', '--timeout', '10'],
                        capture_output=True, timeout=30,
                    )
                except Exception:
                    pass
                try:
                    os.remove(filepath)
                except OSError:
                    pass

    # Stale spark_tmp directories
    for dirpath in glob.glob('work_directory/docker_data/spark_tmp-*'):
        if os.path.isdir(dirpath) and now - os.path.getmtime(dirpath) > max_age_seconds:
            instance_id = _extract_instance_id(dirpath)
            if instance_id and _instance_has_live_containers(instance_id):
                logger.info(
                    f"Skipping live instance temp dir: {dirpath} "
                    f"(container for {instance_id} still running)"
                )
                continue
            logger.warning(f"Cleaning stale temp dir: {dirpath}")
            shutil.rmtree(dirpath, ignore_errors=True)

    # Stale signal directories
    for dirpath in glob.glob('work_directory/docker_data/signals-*'):
        if os.path.isdir(dirpath) and now - os.path.getmtime(dirpath) > max_age_seconds:
            instance_id = _extract_instance_id(dirpath)
            if instance_id and _instance_has_live_containers(instance_id):
                logger.info(
                    f"Skipping live instance signal dir: {dirpath} "
                    f"(container for {instance_id} still running)"
                )
                continue
            logger.warning(f"Cleaning stale signal dir: {dirpath}")
            shutil.rmtree(dirpath, ignore_errors=True)

    # Stale common zip files left in the project root (inside /app mount)
    for filepath in glob.glob('common-*.zip'):
        if now - os.path.getmtime(filepath) > max_age_seconds:
            instance_id = _extract_instance_id(filepath)
            if instance_id and _instance_has_live_containers(instance_id):
                logger.info(
                    f"Skipping live instance zip: {filepath} "
                    f"(container for {instance_id} still running)"
                )
                continue
            logger.warning(f"Cleaning stale zip: {filepath}")
            try:
                os.remove(filepath)
            except OSError:
                pass


###################################################################################################################################
# Cluster Management Functions for Scheduler
###################################################################################################################################

def create_master_docker_compose(input_directory, enable_ui_binding=False):
    """Create docker-compose file with only the master (no workers)."""
    logger = get_logger()
    os.makedirs(spark_tmp_dir, exist_ok=True)
    os.makedirs('work_directory/docker_data/ovid_logs', exist_ok=True)
    os.chmod('work_directory/docker_data', 0o777)
    os.chmod(spark_tmp_dir, 0o777)
    os.chmod('work_directory/docker_data/ovid_logs', 0o777)
    
    host_ip = "10.44.4.4"
    output_file = compose_file_master
    workdir = '../'
    outdir = '../'

    compose = {
        'services': {},
        'networks': {
            'spark_network': {'driver': 'bridge'}
        }
    }

    # Spark Master
    master_ports = []
    if enable_ui_binding:
        master_ports = [
            "8080:8080",
            "7077:7077",
            "4040:4040"
        ]

    compose['services'][master_name] = {
        'image': 'ovid_image_2',
        'container_name': master_name,
        'hostname': master_name,
        'command': [
            "/opt/bitnami/spark/bin/spark-class",
            "org.apache.spark.deploy.master.Master",
            "--host", "0.0.0.0"
        ],
        'environment': [
            'SPARK_MODE=master',
            'SPARK_MASTER_PORT=7077',
            'SPARK_MASTER_WEBUI_PORT=8080',
            f'SPARK_PUBLIC_DNS={host_ip}',
            'PYARROW_IGNORE_TIMEZONE=1',
            'HOME=/tmp'
        ],
        'deploy': {
            'resources': {
                'limits': {'memory': DEFAULT_MASTER_MEMORY},
                'reservations': {'memory': DEFAULT_MASTER_MEMORY}
            }
        },
        'ports': master_ports,
        'volumes': [
            f'{workdir}:/app',
            f'{input_directory}:/data',
            f'{outdir}:/output',
            f'{CREDENTIALS_PATH}:/ovid_credentials',
            './docker_data:/docker_data',
            # Shared Datavant match-ID inbox. Partner hash_token formatters
            # publish timestamped `--to datavant` files here (via
            # commonFunctions.datavant_export_to_datavant_inbox); the
            # downstream match-ID generation reads from the host side.
            # DATAVANT_MATCH_INBOX_DIR in common/settings.py must stay in
            # sync with the container-side path below. Set the host-side
            # directory via DATAVANT_MATCH_HOST_DIR in common/settings.py.
            f'{DATAVANT_MATCH_HOST_DIR}:/app/to_datavant_inbox',
            '/var/run/docker.sock:/var/run/docker.sock',  # Mount Docker socket for scheduler to control workers
            '/usr/bin/docker:/usr/bin/docker',  # Mount Docker CLI binary
            '/usr/local/bin/docker-compose:/usr/local/bin/docker-compose'  # Mount docker-compose binary
        ],
        'networks': ['spark_network'],
        'stop_grace_period': '30s'
    }

    # Write docker-compose-master.yml
    with open(output_file, 'w') as f:
        yaml.dump(compose, f, sort_keys=False)

    logger.info(f"Docker Compose file '{output_file}' created with master only.")
    return output_file


def create_workers_docker_compose(input_directory, workers_count, enable_ui_binding=False):
    """Create docker-compose file with only workers (connects to existing master)."""
    logger = get_logger()
    
    host_ip = "10.44.4.4"
    output_file = compose_file_workers
    workdir = '../'
    outdir = '../'

    # Workers connect to the master's network as an external network
    master_network_name = f'{project_name}_spark_network'
    
    compose = {
        'services': {},
        'networks': {
            'master_network': {
                'external': True,
                'name': master_network_name
            }
        }
    }

    # Spark Workers
    for i in range(1, workers_count + 1):
        worker_name = f'ovid-worker-{i}-{timestamp}'
        host_ui_port = str(8080 + i)

        worker_ports = []
        if enable_ui_binding:
            worker_ports = [
                f"{host_ui_port}:8081"
            ]

        compose['services'][worker_name] = {
            'image': 'ovid_image_2',
            'container_name': worker_name,
            'environment': [
                'SPARK_MODE=worker',
                f'SPARK_MASTER_URL=spark://{master_name}:7077',
                'SPARK_WORKER_CORES=1',
                'SPARK_WORKER_MEMORY=8g',
                f'SPARK_WORKER_WEBUI_PORT=8081',
                f'SPARK_PUBLIC_DNS={host_ip}',
                'PYARROW_IGNORE_TIMEZONE=1',
                'HOME=/tmp'
            ],
            'deploy': {
                'resources': {
                    'limits': {'memory': DEFAULT_WORKER_MEMORY},
                    'reservations': {'memory': DEFAULT_WORKER_MEMORY}
                }
            },
            'ports': worker_ports,
            'volumes': [
                f'{workdir}:/app',
                f'{input_directory}:/data',
                f'{outdir}:/output',
                f'{CREDENTIALS_PATH}:/ovid_credentials',
                './docker_data:/docker_data',
                # Shared Datavant match-ID inbox (see comment in the master
                # spec above and in common/settings.py). Every worker gets
                # the same bind mount because partner formatter subprocesses
                # can be scheduled onto any worker.
                f'{DATAVANT_MATCH_HOST_DIR}:/app/to_datavant_inbox'
            ],
            'networks': ['master_network'],
            'stop_grace_period': '30s'
        }

    # Write docker-compose-workers.yml
    with open(output_file, 'w') as f:
        yaml.dump(compose, f, sort_keys=False)

    logger.info(f"Docker Compose file '{output_file}' created with {workers_count} worker(s).")
    return output_file


def start_master():
    """Start only the Spark master container."""
    logger = get_logger()
    start_cmd = ['docker', 'compose', '-f', compose_file_master, '-p', project_name, 'up', '-d']
    logger.info("Starting Spark master...")
    subprocess.run(" ".join(start_cmd), shell=True)
    logger.info("Spark master started.")


def start_workers(workers_count):
    """Start the Spark worker containers."""
    logger = get_logger()
    start_cmd = ['docker', 'compose', '-f', compose_file_workers, '-p', f'{project_name}-workers', 'up', '-d']
    logger.info(f"Starting {workers_count} Spark worker(s)...")
    subprocess.run(" ".join(start_cmd), shell=True)
    logger.info("Spark workers started.")


def stop_workers():
    """Stop the Spark worker containers."""
    logger = get_logger()
    stop_cmd = ['docker', 'compose', '-f', compose_file_workers, '-p', f'{project_name}-workers', 'down', '--timeout', '30', '--remove-orphans']
    logger.info("Stopping Spark workers...")
    result = subprocess.run(" ".join(stop_cmd), shell=True)
    if result.returncode != 0:
        logger.warning("Worker shutdown had errors, retrying with extended timeout...")
        force_cmd = ['docker', 'compose', '-f', compose_file_workers, '-p', f'{project_name}-workers', 'down', '--timeout', '60', '--remove-orphans', '--volumes']
        subprocess.run(" ".join(force_cmd), shell=True)
    logger.info("Spark workers stopped.")


def stop_master():
    """Stop the Spark master container."""
    logger = get_logger()
    stop_cmd = ['docker', 'compose', '-f', compose_file_master, '-p', project_name, 'down', '--timeout', '30', '--remove-orphans']
    logger.info("Stopping Spark master...")
    result = subprocess.run(" ".join(stop_cmd), shell=True)
    if result.returncode != 0:
        logger.warning("Master shutdown had errors, retrying with extended timeout...")
        force_cmd = ['docker', 'compose', '-f', compose_file_master, '-p', project_name, 'down', '--timeout', '60', '--remove-orphans', '--volumes']
        subprocess.run(" ".join(force_cmd), shell=True)
    logger.info("Spark master stopped.")


def stop_cluster():
    """Stop both workers and master."""
    stop_workers()
    stop_master()


###################################################################################################################################
# 
###################################################################################################################################

def create_docker_compose(input_directory, workers_count, enable_ui_binding=False ):
    logger = get_logger()
    os.makedirs(spark_tmp_dir, exist_ok=True)
    os.makedirs('work_directory/docker_data/ovid_logs', exist_ok=True)
    for path in ['work_directory/docker_data', spark_tmp_dir, 'work_directory/docker_data/ovid_logs']:
        try:
            os.chmod(path, 0o777)
        except PermissionError:
            logger.warning(f"Could not change permissions on {path} - directory may be owned by another user")
    
    host_ip = "10.44.4.4"
    output_file = compose_file
    workdir = '../'
    outdir = '../'

    compose = {
        'services': {},
        'networks': {
            'spark_network': {'driver': 'bridge'}
        }
    }

    # Spark Master
    master_ports = []
    if enable_ui_binding:
        master_ports = [
            "8080:8080",
            "7077:7077",
            "4040:4040"
        ]

    compose['services'][master_name] = {
        'image': 'ovid_image_2',
        'container_name': master_name,
        'hostname': master_name,
        'command': [
            "/opt/bitnami/spark/bin/spark-class",
            "org.apache.spark.deploy.master.Master",
            "--host", "0.0.0.0"
        ],
        'environment': [
            'SPARK_MODE=master',
            'SPARK_MASTER_PORT=7077',
            'SPARK_MASTER_WEBUI_PORT=8080',
            f'SPARK_PUBLIC_DNS={host_ip}',
            'PYARROW_IGNORE_TIMEZONE=1',
            'HOME=/tmp'
        ],
        'deploy': {
            'resources': {
                'limits': {'memory': DEFAULT_MASTER_MEMORY},
                'reservations': {'memory': DEFAULT_MASTER_MEMORY}
            }
        },
        'ports': master_ports,
        'volumes': [
            f'{workdir}:/app',
            f'{input_directory}:/data',
            f'{outdir}:/output',
            f'{CREDENTIALS_PATH}:/ovid_credentials',
            './docker_data:/docker_data',
            # Shared Datavant match-ID inbox (see comment in the master-only
            # compose block earlier in this file and in common/settings.py).
            f'{DATAVANT_MATCH_HOST_DIR}:/app/to_datavant_inbox'
        ],
        'networks': ['spark_network'],
        'stop_grace_period': '30s'
    }

    # Spark Workers
    for i in range(1, workers_count + 1):
        worker_name = f'ovid-worker-{i}-{timestamp}'
        host_ui_port = str(8080 + i)

        worker_ports = []
        if enable_ui_binding:
            worker_ports = [
                f"{host_ui_port}:8081"
            ]

        compose['services'][worker_name] = {
            'image': 'ovid_image_2',
            'container_name': worker_name,
            'depends_on': [master_name],
            'environment': [
                'SPARK_MODE=worker',
                f'SPARK_MASTER_URL=spark://{master_name}:7077',
                'SPARK_WORKER_CORES=1',
                'SPARK_WORKER_MEMORY=8g',
                f'SPARK_WORKER_WEBUI_PORT=8081',
                f'SPARK_PUBLIC_DNS={host_ip}',
                'PYARROW_IGNORE_TIMEZONE=1',
                'HOME=/tmp'
            ],
            'deploy': {
                'resources': {
                    'limits': {'memory': DEFAULT_WORKER_MEMORY},
                    'reservations': {'memory': DEFAULT_WORKER_MEMORY}
                }
            },
            'ports': worker_ports,
            'volumes': [
                f'{workdir}:/app',
                f'{input_directory}:/data',
                f'{outdir}:/output',
                f'{CREDENTIALS_PATH}:/ovid_credentials',
                './docker_data:/docker_data',
                # Shared Datavant match-ID inbox — every worker gets it
                # because partner formatter subprocesses can land on any
                # worker. See common/settings.py::DATAVANT_MATCH_INBOX_DIR.
                f'{DATAVANT_MATCH_HOST_DIR}:/app/to_datavant_inbox'
            ],
            'networks': ['spark_network'],
            'stop_grace_period': '30s'
        }

    # Write docker-compose.yml
    with open(output_file, 'w') as f:
        yaml.dump(compose, f, sort_keys=False)

    logger.info(f"Docker Compose file '{output_file}' created with {workers_count} worker(s).")

    if enable_ui_binding:
        logger.info(f"Master UI: http://{host_ip}:8080/")
        for i in range(1, workers_count + 1):
            logger.info(f"Worker-{i} UI: http://{host_ip}:{8080 + i}/")
    else:
        logger.debug("UI binding disabled — no ports are exposed.")

###################################################################################################################################
# 
###################################################################################################################################

def run_ovid_etl(args):
    # Initialize logging with timestamp and partner
    # Use docker_data/ovid_logs to keep logs in same location as container logs
    setup_logging(timestamp=timestamp, partner=args.partner, base_dir="work_directory/docker_data/ovid_logs")
    logger = get_logger()
    log_job_start("ETL Pipeline", partner=args.partner, command=args.command)

    # Acquire tty-scoped terminal lock so concurrent instances don't corrupt
    # each other's live display.  Second instance on the same tty goes log-only.
    sync_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.ovid_sync')
    os.makedirs(sync_dir, exist_ok=True)
    if not acquire_terminal_lock(sync_dir):
        print(f"\n  *** Another OVID instance is using this terminal. ***")
        print(f"  *** Output redirected to log: {get_log_file_path()} ***\n")

    cleanup_stale_instances()

    input_directory = args.input_directory if args.input_directory is not None else DEFAULT_INPUT_DIRECTORY
    workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT
    create_docker_compose(input_directory, workers_count)



    jobs      = ' '.join(args.job) if args.job is not None else DEFAULT_JOBS
    tables    = ' '.join(args.table) if args.table is not None else DEFAULT_TABLES
    db_server = args.db_server if args.db_server is not None else DEFAULT_DB_SERVER
    db_type   = args.database_type if args.database_type is not None else DEFAULT_DB_TYPE

    partner_settings_path = "partners."+args.partner+".partner_settings"
    partner_settings = importlib.import_module(partner_settings_path)

    db_name   = args.db_name if args.db_name is not None else partner_settings.DEFAULT_PARTNER_DB_NAME
    merge_db_name   = args.merge_database if args.merge_database is not None else partner_settings.MERGE_DATABASE_NAME
    merge_schema_name   = args.merge_schema if args.merge_schema is not None else partner_settings.MERGE_SCHEMA_NAME
    upload_type   = args.upload_type if args.upload_type is not None else partner_settings.DEFAULT_UPLOAD_TYPE

    logger.info(f"ETL Configuration: jobs={jobs}, tables={tables}, db={db_name}, upload_type={upload_type}")

    command =  f"""
        docker exec -i {master_name} bash -c "
        export SPARK_HOME=/opt/bitnami/spark && \
        export SPARK_URL="spark://{master_name}:7077" &&\
        export HOME=/tmp && \
        cd /app && \
        zip -q -j {common_zip_name} common/*.py && \
        /opt/bitnami/spark/bin/spark-submit \
            --conf spark.local.dir=/docker_data/spark_tmp-{timestamp} \
            --conf spark.driver.memoryOverhead={DEFAULT_DRIVER_MEMORY_OVERHEAD} \
            --conf spark.executor.memoryOverhead={DEFAULT_EXECUTOR_MEMORY_OVERHEAD}\
            --conf spark.driver.memory={DEFAULT_DRIVER_MEMORY}\
            --conf spark.sql.adaptive.enabled=true\
            --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///app/common/log4j-quiet.properties' \
            --master spark://{master_name}:7077 \
            --py-files {common_zip_name}   /app/pipelines/{args.command}.py  -f {' '.join(args.ovid_input_folder)} -p {args.partner}  -j {jobs}  -t {tables} -db {db_name} -dt {db_type} -s {db_server} -ut {upload_type} -mdb {merge_db_name} -ms {merge_schema_name} ; \
        rm -f {common_zip_name}
            "
        """



    run_on_cluster(command, folders=args.ovid_input_folder)
    log_job_end("ETL Pipeline", success=True)




###################################################################################################################################
# 
###################################################################################################################################

def run_ovid_analysis(args):
    # Initialize logging with timestamp and partner
    # Use docker_data/ovid_logs to keep logs in same location as container logs
    setup_logging(timestamp=timestamp, partner=args.partner, base_dir="work_directory/docker_data/ovid_logs")
    logger = get_logger()
    log_job_start("Analysis Pipeline", partner=args.partner, command=args.command)

    # Acquire tty-scoped terminal lock so concurrent instances don't corrupt
    # each other's live display.  Second instance on the same tty goes log-only.
    sync_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.ovid_sync')
    os.makedirs(sync_dir, exist_ok=True)
    if not acquire_terminal_lock(sync_dir):
        print(f"\n  *** Another OVID instance is using this terminal. ***")
        print(f"  *** Output redirected to log: {get_log_file_path()} ***\n")

    cleanup_stale_instances()

    input_directory =  DEFAULT_INPUT_DIRECTORY
    workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT
    create_docker_compose(input_directory, workers_count)


    jobs      = ' '.join(args.job) if args.job is not None else DEFAULT_JOBS
    tables    = ' '.join(args.table) if args.table is not None else DEFAULT_TABLES
    fields    = ' '.join(args.field) if getattr(args, 'field', None) is not None else 'all'

    logger.info(f"Analysis Configuration: jobs={jobs}, tables={tables}, fields={fields}")


    command =  f"""
        docker exec -i {master_name} bash -c "
        export SPARK_HOME=/opt/bitnami/spark && \
        export SPARK_URL="spark://{master_name}:7077" &&\
        export HOME=/tmp && \
        cd /app && \
        zip -q -j {common_zip_name} common/*.py && \
        /opt/bitnami/spark/bin/spark-submit \
            --conf spark.local.dir=/docker_data/spark_tmp-{timestamp} \
            --conf spark.driver.memoryOverhead={DEFAULT_DRIVER_MEMORY_OVERHEAD} \
            --conf spark.executor.memoryOverhead={DEFAULT_EXECUTOR_MEMORY_OVERHEAD}\
            --conf spark.driver.memory={DEFAULT_DRIVER_MEMORY}\
            --conf spark.sql.adaptive.enabled=true\
            --conf spark.sql.adaptive.enabled=true\
            --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///app/common/log4j-quiet.properties' \
            --master spark://{master_name}:7077 \
            --py-files {common_zip_name}   /app/pipelines/{args.command}.py  -f {' '.join(args.ovid_input_folder)} -p {args.partner}  -j {jobs}  -t {tables} -fld {fields} ; \
        rm -f {common_zip_name}
            "
        """

    run_on_cluster(command, folders=args.ovid_input_folder, pipeline_name="Analysis")
    log_job_end("Analysis Pipeline", success=True)



###################################################################################################################################
# 
###################################################################################################################################

def run_ovid_dq_checks(args):
    # Initialize logging with timestamp and partner
    # Use docker_data/ovid_logs to keep logs in same location as container logs
    setup_logging(timestamp=timestamp, partner=args.partner, base_dir="work_directory/docker_data/ovid_logs")
    logger = get_logger()
    log_job_start("DQ Checks Pipeline", partner=args.partner, command=args.command)

    # Acquire tty-scoped terminal lock so concurrent instances don't corrupt
    # each other's live display.  Second instance on the same tty goes log-only.
    sync_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.ovid_sync')
    os.makedirs(sync_dir, exist_ok=True)
    if not acquire_terminal_lock(sync_dir):
        print(f"\n  *** Another OVID instance is using this terminal. ***")
        print(f"  *** Output redirected to log: {get_log_file_path()} ***\n")

    cleanup_stale_instances()

    input_directory =  DEFAULT_INPUT_DIRECTORY
    workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT
    create_docker_compose(input_directory, workers_count)

    jobs      = ' '.join(args.job) if args.job is not None else DEFAULT_JOBS
    tables    = ' '.join(args.table) if args.table is not None else DEFAULT_TABLES

    logger.info(f"DQ Checks Configuration: jobs={jobs}, tables={tables}")


    command =  f"""
        docker exec -i {master_name} bash -c "
        export SPARK_HOME=/opt/bitnami/spark && \
        export SPARK_URL="spark://{master_name}:7077" &&\
        export HOME=/tmp && \
        cd /app && \
        zip -q -j {common_zip_name} common/*.py && \
        /opt/bitnami/spark/bin/spark-submit \
            --conf spark.local.dir=/docker_data/spark_tmp-{timestamp} \
            --conf spark.driver.memoryOverhead={DEFAULT_DRIVER_MEMORY_OVERHEAD} \
            --conf spark.executor.memoryOverhead={DEFAULT_EXECUTOR_MEMORY_OVERHEAD}\
            --conf spark.driver.memory={DEFAULT_DRIVER_MEMORY}\
            --conf spark.sql.adaptive.enabled=true\
            --conf spark.sql.adaptive.enabled=true\
            --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///app/common/log4j-quiet.properties' \
            --master spark://{master_name}:7077 \
            --py-files {common_zip_name}   /app/pipelines/{args.command}.py  -f {' '.join(args.ovid_input_folder)} -p {args.partner}  -j {jobs}  -t {tables} ; \
        rm -f {common_zip_name}
            "
        """

    run_on_cluster(command, folders=args.ovid_input_folder, pipeline_name="DQ Checks")
    log_job_end("DQ Checks Pipeline", success=True)


###################################################################################################################################
# 
###################################################################################################################################

def run_ovid_tools(args):
    """Host-side launcher for the OVID Tools pipeline.

    Spins up the standard Ovid Spark cluster (1 master + N workers) and
    invokes ``pipelines/tools.py`` inside the master container with the
    user's job, partner, folder, and table arguments. Currently supports
    only the ``raw_upload`` job, which uploads raw input files (pre-format)
    to Snowflake under ``{db_name}.RAW_{folder_basename}.{TABLE_NAME}``.

    Args:
        args: Parsed argparse namespace from the ``tools`` subparser.
    """
    # Initialize logging with timestamp and partner.
    setup_logging(timestamp=timestamp, partner=args.partner, base_dir="work_directory/docker_data/ovid_logs")
    logger = get_logger()
    log_job_start("Tools Pipeline", partner=args.partner, command=args.command)

    # Acquire tty-scoped terminal lock so concurrent instances don't corrupt
    # each other's live display.  Second instance on the same tty goes log-only.
    sync_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.ovid_sync')
    os.makedirs(sync_dir, exist_ok=True)
    if not acquire_terminal_lock(sync_dir):
        print(f"\n  *** Another OVID instance is using this terminal. ***")
        print(f"  *** Output redirected to log: {get_log_file_path()} ***\n")

    cleanup_stale_instances()

    input_directory = args.input_directory if args.input_directory is not None else DEFAULT_INPUT_DIRECTORY
    workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT
    create_docker_compose(input_directory, workers_count)

    jobs      = ' '.join(args.job) if args.job is not None else 'raw_upload'
    tables    = ' '.join(args.table) if args.table is not None else DEFAULT_TABLES
    db_server = args.db_server if args.db_server is not None else DEFAULT_DB_SERVER
    db_type   = args.database_type if args.database_type is not None else DEFAULT_DB_TYPE

    partner_settings_path = "partners." + args.partner + ".partner_settings"
    partner_settings = importlib.import_module(partner_settings_path)

    db_name = args.db_name if args.db_name is not None else partner_settings.DEFAULT_PARTNER_DB_NAME

    logger.info(
        f"Tools Configuration: jobs={jobs}, tables={tables}, "
        f"db={db_name}, db_type={db_type}, db_server={db_server}"
    )

    command = f"""
        docker exec -i {master_name} bash -c "
        export SPARK_HOME=/opt/bitnami/spark && \
        export SPARK_URL="spark://{master_name}:7077" &&\
        export HOME=/tmp && \
        cd /app && \
        zip -q -j {common_zip_name} common/*.py && \
        /opt/bitnami/spark/bin/spark-submit \
            --conf spark.local.dir=/docker_data/spark_tmp-{timestamp} \
            --conf spark.driver.memoryOverhead={DEFAULT_DRIVER_MEMORY_OVERHEAD} \
            --conf spark.executor.memoryOverhead={DEFAULT_EXECUTOR_MEMORY_OVERHEAD}\
            --conf spark.driver.memory={DEFAULT_DRIVER_MEMORY}\
            --conf spark.sql.adaptive.enabled=true\
            --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///app/common/log4j-quiet.properties' \
            --master spark://{master_name}:7077 \
            --py-files {common_zip_name}   /app/pipelines/{args.command}.py  -f {' '.join(args.ovid_input_folder)} -p {args.partner}  -j {jobs}  -t {tables} -db {db_name} -dt {db_type} -s {db_server} ; \
        rm -f {common_zip_name}
            "
        """

    run_on_cluster(command, folders=args.ovid_input_folder, pipeline_name="Tools")
    log_job_end("Tools Pipeline", success=True)


###################################################################################################################################
# 
###################################################################################################################################

def run_ovid_scheduler(args):
    """Launch the OVID scheduler.

    Two architectural modes are supported, selected by SCHEDULER_MODE
    in common/settings.py:

      * 'orchestrator' (default, since 2026-06-12): runs the host-side
        parallel orchestrator at pipelines/scheduler.py.  No master
        container, no worker_watcher subprocess, no docker compose.
        Each batch is dispatched as a fresh `ovid etl` subprocess
        spawned in its own Spark cluster instance.  N parallel slots.

      * 'legacy': runs the pre-2026-06-12 in-container scheduler at
        pipelines/scheduler_legacy.py.  Spawns a long-lived master
        container, then docker-execs spark-submit to run the legacy
        scheduler inside it.  Sequential one-batch-at-a-time drain.
        Kept for emergency rollback only.
    """
    setup_logging(timestamp=timestamp, partner="scheduler", base_dir="work_directory/docker_data/ovid_logs")
    logger = get_logger()
    log_job_start("Scheduler", partner="scheduler", command=args.command)

    # Acquire tty-scoped terminal lock so concurrent instances don't corrupt
    # each other's live display.  Second instance on the same tty goes log-only.
    sync_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.ovid_sync')
    os.makedirs(sync_dir, exist_ok=True)
    if not acquire_terminal_lock(sync_dir):
        print(f"\n  *** Another OVID instance is using this terminal. ***")
        print(f"  *** Output redirected to log: {get_log_file_path()} ***\n")

    cleanup_stale_instances()

    # Resolve email + raw_upload overrides (used by both modes via env vars).
    email_to_csv = ""
    raw_email_to = getattr(args, "email_to", None) or []
    email_addrs: list[str] = []
    for entry in raw_email_to:
        for piece in str(entry).split(","):
            piece = piece.strip().replace("'", "")
            if piece:
                email_addrs.append(piece)
    if email_addrs:
        email_to_csv = ",".join(email_addrs)
        logger.info(f"Email recipients overridden via --email-to: {email_addrs}")
    email_disabled_flag = "1" if getattr(args, "email_disabled", False) else "0"
    if email_disabled_flag == "1":
        logger.info("Email notifications disabled for this run via --email-disabled")

    raw_upload_disabled_flag = (
        "1" if getattr(args, "raw_upload_disabled", False) else "0"
    )
    if raw_upload_disabled_flag == "1":
        logger.info(
            "raw_upload stage disabled for this run via --raw-upload-disabled"
        )

    # Export overrides as environment variables so the orchestrator
    # (host-side) and the legacy scheduler (in-container, via docker exec)
    # both see them identically.
    os.environ["OVID_EMAIL_RECIPIENTS"] = email_to_csv
    os.environ["OVID_EMAIL_DISABLED"] = email_disabled_flag
    os.environ["OVID_RAW_UPLOAD_DISABLED"] = raw_upload_disabled_flag

    # Dispatch by mode.  Default is 'orchestrator'.
    mode = (getattr(args, "scheduler_mode", None)
            or os.environ.get("OVID_SCHEDULER_MODE")
            or SCHEDULER_MODE).strip().lower()

    if mode == "orchestrator":
        try:
            _run_orchestrator_mode(logger)
        finally:
            release_terminal_lock()
            log_job_end("Scheduler", success=True)
        return

    if mode == "legacy":
        _run_legacy_scheduler_mode(args, logger, email_to_csv,
                                   email_disabled_flag,
                                   raw_upload_disabled_flag)
        return

    logger.error(
        f"Unknown SCHEDULER_MODE='{mode}'; valid values are 'orchestrator' "
        f"or 'legacy'.  Defaulting to orchestrator."
    )
    try:
        _run_orchestrator_mode(logger)
    finally:
        release_terminal_lock()
        log_job_end("Scheduler", success=True)


def _run_orchestrator_mode(logger):
    """Run the new host-side parallel orchestrator (default since 2026-06-12).

    The orchestrator does NOT spawn a master container.  It runs entirely
    on the host as a Python process and dispatches each batch to a free
    slot, where the slot spawns a fresh `ovid etl` subprocess (which
    itself uses ovid.py's existing instance isolation to bring up its own
    Spark cluster).
    """
    logger.info(
        f"Starting OVID Scheduler (orchestrator mode); "
        f"slots={SCHEDULER_PARALLEL_SLOTS}, "
        f"workers_per_slot={SCHEDULER_WORKERS_PER_SLOT}"
    )

    # Import lazily so the legacy code path doesn't have to satisfy the
    # orchestrator's host-only dependencies (snowflake.connector,
    # cryptography).  Both are already pip-installed on the host but
    # being explicit keeps the legacy path's failure modes simpler.
    sys_path_added = False
    pipelines_dir = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'pipelines'
    )
    if pipelines_dir not in sys.path:
        sys.path.insert(0, pipelines_dir)
        sys_path_added = True
    try:
        import scheduler as orchestrator_module
        orchestrator_module.run_orchestrator()
    except KeyboardInterrupt:
        logger.info("Orchestrator interrupted by user.")
    except Exception as e:
        logger.error(f"Orchestrator main loop crashed: {e}", exc_info=True)
    finally:
        if sys_path_added:
            try:
                sys.path.remove(pipelines_dir)
            except ValueError:
                pass


def _run_legacy_scheduler_mode(args, logger, email_to_csv,
                               email_disabled_flag, raw_upload_disabled_flag):
    """Run the pre-2026-06-12 in-container scheduler (legacy fallback).

    Brings up a master container + worker_watcher process, then docker-execs
    spark-submit to launch pipelines/scheduler_legacy.py inside the master.
    Set SCHEDULER_MODE = 'legacy' in common/settings.py to use this path.
    """
    logger.info("Starting OVID Scheduler (LEGACY in-container mode)...")

    input_directory = args.input_directory if args.input_directory is not None else DEFAULT_INPUT_DIRECTORY
    workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT

    # Create separate docker-compose files for master and workers
    create_master_docker_compose(input_directory)
    create_workers_docker_compose(input_directory, workers_count)

    # Create instance-scoped signal directory
    os.makedirs(signal_dir, exist_ok=True)
    os.chmod(signal_dir, 0o777)

    # Start only the master initially (no workers)
    start_master()
    # Give master time to start
    time.sleep(5)

    # Start the worker watcher in a separate process
    import multiprocessing
    watcher_process = multiprocessing.Process(
        target=_run_worker_watcher,
        args=(project_name, workers_count, compose_file_workers, signal_dir)
    )
    watcher_process.start()
    logger.info(f"Worker watcher started (PID: {watcher_process.pid})")

    try:
        # Run the scheduler inside the master container.
        # NOTE: the legacy scheduler script is now scheduler_legacy.py to
        # leave the orchestrator at the canonical scheduler.py path.
        command = f"""
            docker exec -i {master_name} bash -c "
            export SPARK_HOME=/opt/bitnami/spark && \
            export SPARK_URL='spark://{master_name}:7077' && \
            export HOME=/tmp && \
            export OVID_SIGNAL_DIR='/docker_data/signals-{timestamp}' && \
            export OVID_INSTANCE_ID='{timestamp}' && \
            export OVID_SPARK_TMP='/docker_data/spark_tmp-{timestamp}' && \
            export OVID_EMAIL_RECIPIENTS='{email_to_csv}' && \
            export OVID_EMAIL_DISABLED='{email_disabled_flag}' && \
            export OVID_RAW_UPLOAD_DISABLED='{raw_upload_disabled_flag}' && \
            cd /app && \
            zip -q -j {common_zip_name} common/*.py && \
            /opt/bitnami/spark/bin/spark-submit \
                --conf spark.local.dir=/docker_data/spark_tmp-{timestamp} \
                --conf spark.driver.memoryOverhead={DEFAULT_DRIVER_MEMORY_OVERHEAD} \
                --conf spark.executor.memoryOverhead={DEFAULT_EXECUTOR_MEMORY_OVERHEAD} \
                --conf spark.driver.memory={DEFAULT_DRIVER_MEMORY} \
                --conf spark.sql.adaptive.enabled=true \
                --conf 'spark.driver.extraJavaOptions=-Dlog4j.configuration=file:///app/common/log4j-quiet.properties' \
                --master spark://{master_name}:7077 \
                --py-files {common_zip_name} /app/pipelines/scheduler_legacy.py ; \
            rm -f {common_zip_name}
                "
            """
        run_and_log_subprocess(command, shell=True, label="Scheduler (legacy)")

    except KeyboardInterrupt:
        logger.info("Scheduler interrupted by user.")
    finally:
        # Signal watcher to shutdown and stop everything
        logger.info("Shutting down...")
        shutdown_signal_file = os.path.join(signal_dir, "shutdown")
        with open(shutdown_signal_file, 'w') as f:
            f.write("")
        watcher_process.join(timeout=30)
        if watcher_process.is_alive():
            logger.warning("Worker watcher did not exit cleanly, terminating...")
            watcher_process.terminate()
        stop_master()
        for f in [compose_file_master, compose_file_workers]:
            try:
                os.remove(f)
            except OSError:
                pass
        shutil.rmtree(spark_tmp_dir, ignore_errors=True)
        shutil.rmtree(signal_dir, ignore_errors=True)
        release_terminal_lock()
        log_job_end("Scheduler", success=True)


def _run_worker_watcher(project_name, workers_count, compose_file_path, signal_dir_path):
    """Run the worker watcher (called in a separate process).

    Args:
        project_name: Docker compose project name.
        workers_count: Number of Spark workers to manage.
        compose_file_path: Path to the instance-scoped workers compose file.
        signal_dir_path: Path to the instance-scoped signal directory.
    """
    import sys
    sys.path.insert(0, '.')
    from worker_watcher import run_watcher
    run_watcher(project_name, workers_count, compose_file_path, signal_dir_path)



if __name__ == "__main__":


    parser = argparse.ArgumentParser(
        prog="ovid",
        description="Run OVID ETL and Diagnosis jobs"
    )
    subparsers = parser.add_subparsers(dest="command", required=True, help="Choose a command to run")



    # ---- OVID ETL ----
    etl_parser = subparsers.add_parser("etl", help="Run OVID ETL jobs")
    etl_parser.add_argument("-j", "--job", choices=["all", "format", "deduplicate", "map", "fix", "upload"],nargs="+", required=False)
    etl_parser.add_argument("-f", "--ovid_input_folder",nargs="+", required=True)
    etl_parser.add_argument("-p", "--partner", required=True)
    etl_parser.add_argument("-t", "--table", nargs="+", required=False)
    etl_parser.add_argument("-s", "--db_server", required=False)
    etl_parser.add_argument("-db", "--db_name", required=False)
    etl_parser.add_argument("-dt", "--database_type", required=False)
    etl_parser.add_argument("-id", "--input_directory", required=False)
    etl_parser.add_argument("-wc", "--workers_count", required=False)
    etl_parser.add_argument("-ut", "--upload_type", required=False)
    etl_parser.add_argument("-mdb", "--merge_database", required=False)
    etl_parser.add_argument("-ms", "--merge_schema", required=False)


    etl_parser.set_defaults(func=run_ovid_etl)



    # ---- OVID ANALYSIS ----
    analysis_parser = subparsers.add_parser("analysis", help="Run OVID Analysis jobs")
    analysis_parser.add_argument("-j", "--job", choices=["all", "format", "monthly_counts", "distinct_values", "raw_distinct_values"],nargs="+", required=False)
    analysis_parser.add_argument("-f", "--ovid_input_folder",nargs="+", required=True)
    analysis_parser.add_argument("-p", "--partner", required=True)
    analysis_parser.add_argument("-t", "--table", nargs="+", required=False)
    analysis_parser.add_argument("-fld", "--field", nargs="+", required=False, help="Field/column patterns for raw_distinct_values (supports glob wildcards). Default: all")
    analysis_parser.add_argument("-wc", "--workers_count", required=False)
    analysis_parser.set_defaults(func=run_ovid_analysis)

    # ---- OVID DQ CHECKS ----
    dq_checks_parser = subparsers.add_parser("dq_checks", help="Run OVID Data Quality Checks jobs")
    dq_checks_parser.add_argument("-j", "--job", choices=["all", "format", "mapping_gap", "mapping_report","dc_checks"],nargs="+", required=False)
    dq_checks_parser.add_argument("-f", "--ovid_input_folder",nargs="+", required=True)
    dq_checks_parser.add_argument("-p", "--partner", required=True)
    dq_checks_parser.add_argument("-t", "--table", nargs="+", required=False)
    dq_checks_parser.add_argument("-wc", "--workers_count", required=False)
    dq_checks_parser.set_defaults(func=run_ovid_dq_checks)

    # ---- OVID SCHEDULER ----
    scheduler_parser = subparsers.add_parser("scheduler", help="Run OVID Scheduler to process data batches")
    scheduler_parser.add_argument("-id", "--input_directory", required=False)
    scheduler_parser.add_argument("-wc", "--workers_count", required=False)
    scheduler_parser.add_argument(
        "--email-to",
        dest="email_to",
        action="append",
        default=None,
        help=(
            "Override the batch completion email recipients for this run "
            "(useful for testing). Pass a single email, a comma-separated "
            "list, or repeat the flag. Replaces (does not extend) the "
            "default SCHEDULER_EMAIL_RECIPIENTS from settings.py. "
            "Precedence: --email-to > OVID_EMAIL_RECIPIENTS env var > "
            "SCHEDULER_EMAIL_RECIPIENTS in common/settings.py."
        ),
    )
    scheduler_parser.add_argument(
        "--email-disabled",
        dest="email_disabled",
        action="store_true",
        help=(
            "Disable batch completion emails for this run "
            "(overrides SCHEDULER_EMAIL_ENABLED). Equivalent to "
            "OVID_EMAIL_DISABLED=1."
        ),
    )
    scheduler_parser.add_argument(
        "--raw-upload-disabled",
        dest="raw_upload_disabled",
        action="store_true",
        help=(
            "Skip the post-ETL raw_upload stage for every batch in this "
            "run (overrides SCHEDULER_RAW_UPLOAD_ENABLED). When the "
            "raw_upload stage is enabled, the scheduler uploads every "
            "csv/tsv/txt file under /data/{partner}/{folder}/ to "
            "{char_partner}.RAW_{folder}.{TABLE_NAME} after each batch's "
            "ETL pipeline finishes. raw_upload failures never flip the "
            "batch status from PROCESSED to ERROR. Equivalent to "
            "OVID_RAW_UPLOAD_DISABLED=1."
        ),
    )
    scheduler_parser.add_argument(
        "--mode",
        dest="scheduler_mode",
        choices=["orchestrator", "legacy"],
        default=None,
        help=(
            "Override the scheduler architecture for this run. "
            "'orchestrator' (default since 2026-06-12) runs the host-side "
            "parallel orchestrator with N slots; 'legacy' runs the "
            "pre-2026-06-12 in-container sequential scheduler "
            "(emergency rollback path). Falls back to SCHEDULER_MODE "
            "in common/settings.py when this flag is omitted."
        ),
    )
    scheduler_parser.set_defaults(func=run_ovid_scheduler)


    # ---- OVID TOOLS ----
    # Namespace for ad-hoc cluster utilities that sit outside the standard
    # ETL / analysis / DQ-checks workflow. Each utility is exposed as a
    # ``-j`` choice; the first such utility is ``raw_upload`` which uploads
    # raw input files (pre-format) to Snowflake under
    # ``{db_name}.RAW_{folder_basename}.{TABLE_NAME}``.
    tools_parser = subparsers.add_parser("tools", help="Run OVID utility tools")
    tools_parser.add_argument("-j", "--job", choices=["raw_upload"], nargs="+", required=True)
    tools_parser.add_argument("-f", "--ovid_input_folder", nargs="+", required=True)
    tools_parser.add_argument("-p", "--partner", required=True)
    tools_parser.add_argument("-t", "--table", nargs="+", required=False)
    tools_parser.add_argument("-s", "--db_server", required=False)
    tools_parser.add_argument("-db", "--db_name", required=False)
    tools_parser.add_argument("-dt", "--database_type", required=False)
    tools_parser.add_argument("-id", "--input_directory", required=False)
    tools_parser.add_argument("-wc", "--workers_count", required=False)
    tools_parser.set_defaults(func=run_ovid_tools)


    args = parser.parse_args()

    args.func(args)

    # input_directory = args.input_directory if args.input_directory is not None else DEFAULT_INPUT_DIRECTORY
    # workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT



    # create_docker_compose(input_directory, workers_count)







