
Developed and maintained by **OneFlorida+**

**Contacts:**
- Abdelali Nouina - abdelali@ufl.edu
- Alexander T. Loiacono - atloiaco@ufl.edu

Last Updated: 2/18/2026

# Ovid

Ovid is a PySpark-based ETL (Extract, Transform, Load) pipeline designed for transforming and harmonizing healthcare data into standardized Common Data Models (CDM), including PCORnet and OMOP formats. The tool automates the process of ingesting raw partner data, applying formatting rules, deduplication, mapping to standard vocabularies, data quality checks, and uploading to a centralized data warehouse.

## Workflow

The typical Ovid workflow consists of the following stages:

```
┌─────────────┐    ┌─────────────┐    ┌───────────────┐    ┌─────────────┐    ┌─────────────┐    ┌─────────────┐
│  Raw Data   │───>│   Format    │───>│  Deduplicate  │───>│     Map     │───>│     Fix     │───>│   Upload    │
│  (Input)    │    │             │    │               │    │             │    │             │    │  (Database) │
└─────────────┘    └─────────────┘    └───────────────┘    └─────────────┘    └─────────────┘    └─────────────┘
```

1. **Format**: Converts raw partner data into a standardized format, applying partner-specific formatting rules
2. **Deduplicate**: Removes duplicate records from the formatted data
3. **Map**: Maps source values to standard vocabularies and CDM conventions
4. **Fix**: Applies partner-specific fixes and corrections to the mapped data
5. **Upload**: Uploads the final processed data to the target database (Snowflake)

Additional pipelines are available for:
- **Analysis**: Generate monthly counts and distinct value reports
- **Data Quality Checks**: Run mapping gap reports, mapping reports, and data quality validations
- **Scheduler**: Automated batch processing of incoming data

# Requirements

This script requires the use of a PySpark cluster. Before running the script, make sure you have set up a PySpark cluster environment.

## Supported Databases

**Currently, Ovid only supports Snowflake for database uploads.** Support for PostgreSQL and Microsoft SQL Server is planned for future implementation.

## Setting Up a PySpark Cluster

# Installation

Before installing Ovid, you must configure the installation settings:

1. Copy the sample settings file:

        cp ovid_setup/install_settings_sample.py ovid_setup/install_settings.py

2. Edit `ovid_setup/install_settings.py` and configure the following:
   - **Paths**: Set `CREDENTIALS_PATH`, `OVID_INSTALL_PATH`, and `OMOP_CONCEPT_TABLES_PATH`
   - **Snowflake settings**: Configure `DB_SERVER_NAME`, `SF_STAGE_NAME`, `SF_ROLE`, `SF_DB_USER`, `SF_WAREHOUSE`, and key file names
   - **Encryption settings**: Set `SEED_FILE_NAME`
   - **Datavant settings**: Configure `DV_CREDENTIALS_FILE_NAME` and `DV_TRANSFORMATION_SCRIPT_FILE_NAME`

3. Run the installer from the project root directory:

        python3 ovid_setup/ovid_install.py

The installer will:
1. Verify Docker is installed
2. Build and load the Docker image
3. Create the `ovid_secrets.py` configuration file
4. Copy OMOP vocabulary tables

You can also use the installer to create new partners from predefined templates (OMOP 5.3, OneFL OMOP-Like, or PCORnet CDM).

# Running Ovid

Ovid uses a command-line interface with subcommands for different pipelines.

## Before running your scripts:

1. Change permissions on all the folders and files in the repository to 777 by going to the upper folder and running the following command:

        chmod -R 777 .


## Command Structure

        python3 ovid.py <command> [options]

Available commands:
- `etl` - Run ETL jobs (format, deduplicate, map, fix, upload)
- `analysis` - Run analysis jobs (monthly_counts, distinct_values)
- `dq_checks` - Run data quality checks (format, mapping_gap, mapping_report)
- `scheduler` - Run the scheduler to process data batches


## Default Parameters

Ovid uses a two-level default parameter system. If a parameter is not provided on the command line, Ovid will first check the partner-specific settings, then fall back to global defaults.

### Global Defaults (common/settings.py)

These defaults apply to all partners unless overridden:

| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| `DEFAULT_WORKERS_COUNT` | 25 | Number of Spark workers |
| `DEFAULT_MASTER_MEMORY` | 8g | Memory allocated to Spark master |
| `DEFAULT_WORKER_MEMORY` | 16g | Memory allocated to each Spark worker |
| `DEFAULT_INPUT_DIRECTORY` | /data/inboxarchive | Default input data directory |
| `DEFAULT_JOBS` | all | Default job(s) to run |
| `DEFAULT_TABLES` | all | Default table(s) to process |
| `DEFAULT_DB_TYPE` | sf | Default database type (Snowflake) |
| `DEFAULT_DB_SERVER` | ufoneflorida-azure_eastus2 | Default database server |

### Partner-Specific Defaults (partners/[partner_name]/partner_settings.py)

Each partner can override defaults in their own `partner_settings.py`:

| Parameter | Description |
|-----------|-------------|
| `DEFAULT_PARTNER_DB_NAME` | Default database name for this partner |
| `DEFAULT_UPLOAD_TYPE` | Upload type: `merge` or `no_merge` |
| `MERGE_DATABASE_NAME` | Database name for merge operations |
| `MERGE_SCHEMA_NAME` | Schema name for merge operations |

### Parameter Resolution Order

When running a command, parameters are resolved in the following order (highest priority first):
1. **Command-line arguments** - Values passed directly in the command (e.g., `-j map` overrides the default `-j all`)
2. **Partner-specific settings** (`partners/[partner]/partner_settings.py`)
3. **Global defaults** (`common/settings.py`)

For example, if `DEFAULT_JOBS = 'all'` is set in settings.py, but you run:

        python3 ovid.py etl -p partnerA -f q2_2023 -j map

The job will be `map`, not `all`, because command-line arguments take priority.


# ETL Pipeline

## To run formatters:

        python3 ovid.py etl -p [partner_name] -f [folder_name] -t [table_name] -j format

                e.g.   python3 ovid.py etl -p partnerA -f q2_2023 -t demographic -j format


## To run deduplication:

        python3 ovid.py etl -p [partner_name] -f [folder_name] -t [table_name] -j deduplicate

                e.g.   python3 ovid.py etl -p partnerA -f q2_2023 -t demographic -j deduplicate


## To run mappers:

        python3 ovid.py etl -p [partner_name] -f [folder_name] -t [table_name] -j map

                e.g.   python3 ovid.py etl -p partnerA -f q2_2023 -t demographic -j map


## To run fixers:

        python3 ovid.py etl -p [partner_name] -f [folder_name] -t [table_name] -j fix

                e.g.   python3 ovid.py etl -p partnerA -f q2_2023 -t demographic -j fix


## To run uploaders:

        python3 ovid.py etl -p [partner_name] -f [folder_name] -t [table_name] -j upload -s [server_name] -db [db_name] -dt [database_type]

                e.g.   python3 ovid.py etl -p partnerA -f q2_2023 -t demographic -j upload -s data_server@foo.edu -db partnerA_db -dt sf


## To run all ETL jobs at once:

        python3 ovid.py etl -p [partner_name] -f [folder_name] -t all -j all -s [server_name] -db [db_name] -dt [database_type]

                e.g.   python3 ovid.py etl -p partnerA -f q2_2023 -t all -j all -s data_server@foo.edu -db partnerA_db -dt sf


## To run multiple jobs or tables:

        python3 ovid.py etl -p partnerA -f q2_2023 -t demographic encounter -j format map


# Analysis Pipeline

## To run monthly counts:

        python3 ovid.py analysis -p [partner_name] -f [folder_name] -t [table_name] -j monthly_counts

                e.g.   python3 ovid.py analysis -p partnerA -f q2_2023 -t demographic -j monthly_counts


## To run distinct values analysis:

        python3 ovid.py analysis -p [partner_name] -f [folder_name] -t [table_name] -j distinct_values

                e.g.   python3 ovid.py analysis -p partnerA -f q2_2023 -t demographic -j distinct_values


# Data Quality Checks Pipeline

## To run mapping gap report:

        python3 ovid.py dq_checks -p [partner_name] -f [folder_name] -t [table_name] -j mapping_gap

                e.g.   python3 ovid.py dq_checks -p partnerA -f q2_2023 -t demographic -j mapping_gap


## To run mapping report:

        python3 ovid.py dq_checks -p [partner_name] -f [folder_name] -t [table_name] -j mapping_report

                e.g.   python3 ovid.py dq_checks -p partnerA -f q2_2023 -t demographic -j mapping_report


## To run data quality checks (in development):

        python3 ovid.py dq_checks -p [partner_name] -f [folder_name] -t [table_name] -j dc_checks

                e.g.   python3 ovid.py dq_checks -p partnerA -f q2_2023 -t demographic -j dc_checks

**Note:** This feature is still in development.


# Parameter Reference

## ETL Parameters

| Parameter | Description |
|-----------|-------------|
| `-p, --partner` | Partner or site name (required). Used to pull partner/site custom dictionaries (e.g., usf, uab) |
| `-f, --ovid_input_folder` | Input folder name(s) where raw data resides (required) |
| `-t, --table` | Table name(s) to run the job on (e.g., demographic, encounter, or all) |
| `-j, --job` | Job type: `all`, `format`, `deduplicate`, `map`, `fix`, `upload` |
| `-s, --db_server` | Database server address or Snowflake account |
| `-db, --db_name` | Database name for upload |
| `-dt, --database_type` | Database type: `sf` (Snowflake). **Note: Only Snowflake is currently supported. PostgreSQL (`pg`) and Microsoft SQL Server (`mssql`) are planned for future implementation.** |
| `-id, --input_directory` | Path to the data parent folder |
| `-wc, --workers_count` | Number of Spark workers |
| `-ut, --upload_type` | Upload type |
| `-mdb, --merge_database` | Merge database name |
| `-ms, --merge_schema` | Merge schema name |

## Analysis Parameters

| Parameter | Description |
|-----------|-------------|
| `-p, --partner` | Partner or site name (required) |
| `-f, --ovid_input_folder` | Input folder name(s) (required) |
| `-t, --table` | Table name(s) |
| `-j, --job` | Job type: `all`, `monthly_counts`, `distinct_values` |
| `-wc, --workers_count` | Number of Spark workers |

## DQ Checks Parameters

| Parameter | Description |
|-----------|-------------|
| `-p, --partner` | Partner or site name (required) |
| `-f, --ovid_input_folder` | Input folder name(s) (required) |
| `-t, --table` | Table name(s) |
| `-j, --job` | Job type: `all`, `format`, `mapping_gap`, `mapping_report`, `dc_checks` (in development) |
| `-wc, --workers_count` | Number of Spark workers |


# Ovid Scheduler 

In order to run Ovid in scheduling mode:

1. Make sure to update the scheduler settings section in the settings.py:

        SCHEDULER_MAX_THREADS= 1   # how many threads to be run at any single time
        SCHEDULER_POLL_INTERVAL_IN_MINUTES = 15  # Interval of how long the scheduler will go to sleep in minutes
        SCHEDULER_DB_TYPE   = 'sf'     # Type of database. Currently only supporting Snowflake
        SCHEDULER_DB_SERVER = '[server_name]'  
        SCHEDULER_DB_NAME   = 'OVID'
        SCHEDULER_DB_SCHEMA    = 'SCHEDULER'
        MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES = 5  # The duration of how long the scheduler will wait for no modification to occur on a data set before running the process

2. Use the following command to run the scheduler:

        python3 ovid.py scheduler -id [path to main folder where the scheduler will be monitoring]

                e.g.   python3 ovid.py scheduler -id /ovid_data
