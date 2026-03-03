
import subprocess
from common.settings import *
import time
import argparse
import yaml
from common.ovid_secrets import CREDENTIALS_PATH
from common.ovid_logging import setup_logging, get_logger, log_job_start, log_job_end
import importlib


from datetime import datetime

timestamp = datetime.now().strftime("%Y%m%d-%H%M%S")

master_name = f'ovid-master-{timestamp}'



###################################################################################################################################
# 
###################################################################################################################################
def run_on_cluster (command):
    logger = get_logger()
    run_cluster_cmd       =  ['docker', 'compose', '-f', 'work_directory/docker-compose.yml', '-p',f'ovid-{timestamp}', 'up', '-d']
    shutdown_cluster_cmd  =  ['docker', 'compose', '-f', 'work_directory/docker-compose.yml', '-p',f'ovid-{timestamp}', 'down']




    try:
        logger.info("Starting Spark cluster...")
        subprocess.run(" ".join(run_cluster_cmd), shell=True)
        logger.info("Executing command on cluster...")
        subprocess.run(command, shell=True)
        logger.info("Shutting down Spark cluster...")
        subprocess.run(" ".join(shutdown_cluster_cmd), shell=True)
        logger.info("Cluster shutdown complete.")
    
    except Exception as e:
        logger.error(f"Error during cluster execution: {e}", exc_info=True)
        subprocess.run(" ".join(shutdown_cluster_cmd), shell=True)

###################################################################################################################################
# 
###################################################################################################################################

def create_docker_compose(input_directory, workers_count, enable_ui_binding=False ):
    logger = get_logger()
    import os
    os.makedirs('work_directory/docker_data/spark_tmp', exist_ok=True)
    os.makedirs('work_directory/docker_data/ovid_logs', exist_ok=True)
    os.chmod('work_directory/docker_data', 0o777)
    os.chmod('work_directory/docker_data/spark_tmp', 0o777)
    os.chmod('work_directory/docker_data/ovid_logs', 0o777)
    
    host_ip = "10.44.4.4"
    output_file = "work_directory/docker-compose.yml"
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
            './docker_data:/docker_data'
        ],
        'networks': ['spark_network']
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
                './docker_data:/docker_data'
            ],
            'networks': ['spark_network']
        }

    # Spark Submit
    compose['services']['spark-submit'] = {
        'image': 'ovid_image_2',
        'container_name': f'ovid-submit-{timestamp}',
        'depends_on': [master_name] + [f'ovid-worker-{i}-{timestamp}' for i in range(1, workers_count + 1)],
        'entrypoint': ['/bin/bash', '-c'],
        'command': [
            f'/opt/bitnami/spark/bin/spark-submit --master spark://{master_name}:7077 --py-files /app/common/commonFunctions.py'
        ],
        'volumes': [
            f'{workdir}:/app',
            f'{input_directory}:/data',
            f'{outdir}:/output',
            f'{CREDENTIALS_PATH}:/ovid_credentials',
            './docker_data:/docker_data'
        ],
        'networks': ['spark_network']
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
        docker exec -it {master_name} bash -c "
        export SPARK_HOME=/opt/bitnami/spark && \
        export SPARK_URL="spark://{master_name}:7077" &&\
        export HOME=/tmp && \
        cd /app && \
        zip -j common.zip common/*.py && \
        /opt/bitnami/spark/bin/spark-submit \
            --conf spark.local.dir=/docker_data/spark_tmp \
            --conf spark.driver.memoryOverhead=2g \
            --conf spark.executor.memoryOverhead=4g\
            --conf spark.driver.memory=32g\
            --conf spark.sql.adaptive.enabled=true\
            --master spark://{master_name}:7077 \
            --py-files common.zip   /app/pipelines/{args.command}.py  -f {' '.join(args.ovid_input_folder)} -p {args.partner}  -j {jobs}  -t {tables} -db {db_name} -dt {db_type} -s {db_server} -ut {upload_type} -mdb {merge_db_name} -ms {merge_schema_name} ; \
        rm -f common.zip
            "
        """



    run_on_cluster(command)
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

    input_directory =  DEFAULT_INPUT_DIRECTORY
    workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT
    create_docker_compose(input_directory, workers_count)


    jobs      = ' '.join(args.job) if args.job is not None else DEFAULT_JOBS
    tables    = ' '.join(args.table) if args.table is not None else DEFAULT_TABLES

    logger.info(f"Analysis Configuration: jobs={jobs}, tables={tables}")


    command =  f"""
        docker exec -it {master_name} bash -c "
        export SPARK_HOME=/opt/bitnami/spark && \
        export SPARK_URL="spark://{master_name}:7077" &&\
        export HOME=/tmp && \
        cd /app && \
        zip -j common.zip common/*.py && \
        /opt/bitnami/spark/bin/spark-submit \
            --conf spark.local.dir=/docker_data/spark_tmp \
            --conf spark.driver.memoryOverhead=2g \
            --conf spark.executor.memoryOverhead=4g\
            --conf spark.driver.memory=32g\
            --conf spark.sql.adaptive.enabled=true\
            --conf spark.sql.adaptive.enabled=true\
            --master spark://{master_name}:7077 \
            --py-files common.zip   /app/pipelines/{args.command}.py  -f {' '.join(args.ovid_input_folder)} -p {args.partner}  -j {jobs}  -t {tables} ; \
        rm -f common.zip
            "
        """

    run_on_cluster(command)
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

    input_directory =  DEFAULT_INPUT_DIRECTORY
    workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT
    create_docker_compose(input_directory, workers_count)

    jobs      = ' '.join(args.job) if args.job is not None else DEFAULT_JOBS
    tables    = ' '.join(args.table) if args.table is not None else DEFAULT_TABLES

    logger.info(f"DQ Checks Configuration: jobs={jobs}, tables={tables}")


    command =  f"""
        docker exec -it {master_name} bash -c "
        export SPARK_HOME=/opt/bitnami/spark && \
        export SPARK_URL="spark://{master_name}:7077" &&\
        export HOME=/tmp && \
        cd /app && \
        zip -j common.zip common/*.py && \
        /opt/bitnami/spark/bin/spark-submit \
            --conf spark.local.dir=/docker_data/spark_tmp \
            --conf spark.driver.memoryOverhead=2g \
            --conf spark.executor.memoryOverhead=4g\
            --conf spark.driver.memory=32g\
            --conf spark.sql.adaptive.enabled=true\
            --conf spark.sql.adaptive.enabled=true\
            --master spark://{master_name}:7077 \
            --py-files common.zip   /app/pipelines/{args.command}.py  -f {' '.join(args.ovid_input_folder)} -p {args.partner}  -j {jobs}  -t {tables} ; \
        rm -f common.zip
            "
        """

    run_on_cluster(command)
    log_job_end("DQ Checks Pipeline", success=True)


###################################################################################################################################
# 
###################################################################################################################################

def run_ovid_scheduler(args):
    setup_logging(timestamp=timestamp, partner="scheduler", base_dir="work_directory/docker_data/ovid_logs")
    logger = get_logger()
    log_job_start("Scheduler", partner="scheduler", command=args.command)

    input_directory = args.input_directory if args.input_directory is not None else DEFAULT_INPUT_DIRECTORY
    workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT
    create_docker_compose(input_directory, workers_count)

    logger.info("Starting OVID Scheduler...")

    command = f"""
        docker exec -it {master_name} bash -c "
        export SPARK_HOME=/opt/bitnami/spark && \
        export SPARK_URL="spark://{master_name}:7077" &&\
        export HOME=/tmp && \
        cd /app && \
        zip -j common.zip common/*.py && \
        /opt/bitnami/spark/bin/spark-submit \
            --conf spark.local.dir=/docker_data/spark_tmp \
            --conf spark.driver.memoryOverhead=2g \
            --conf spark.executor.memoryOverhead=4g\
            --conf spark.driver.memory=32g\
            --conf spark.sql.adaptive.enabled=true\
            --master spark://{master_name}:7077 \
            --py-files common.zip /app/pipelines/scheduler.py ; \
        rm -f common.zip
            "
        """

    run_on_cluster(command)
    log_job_end("Scheduler", success=True)



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
    analysis_parser.add_argument("-j", "--job", choices=["all", "monthly_counts", "distinct_values"],nargs="+", required=False)
    analysis_parser.add_argument("-f", "--ovid_input_folder",nargs="+", required=True)
    analysis_parser.add_argument("-p", "--partner", required=True)
    analysis_parser.add_argument("-t", "--table", nargs="+", required=False)
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
    scheduler_parser.set_defaults(func=run_ovid_scheduler)


    args = parser.parse_args()

    args.func(args)

    # input_directory = args.input_directory if args.input_directory is not None else DEFAULT_INPUT_DIRECTORY
    # workers_count = int(args.workers_count) if args.workers_count is not None else DEFAULT_WORKERS_COUNT



    # create_docker_compose(input_directory, workers_count)







