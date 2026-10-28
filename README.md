
Developed and maintained by **OneFlorida+**

**Contacts:**
- Abdelali Nouina - abdelali@ufl.edu
- Alexander T. Loiacono - atloiaco@ufl.edu

Last Updated: 10/28/2026

# Ovid

Ovid is a PySpark-based ETL (Extract, Transform, Load) pipeline designed for transforming and harmonizing healthcare data into standardized Common Data Models (CDM), including PCORnet and OMOP formats. The tool automates the process of ingesting raw partner data, applying formatting rules, deduplication, mapping to standard vocabularies, data quality checks, and writing the final CDM tables as tab-delimited files (`fixed_{table}.csv`), which can optionally be uploaded to a data warehouse (Snowflake).

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
5. **Upload** *(optional)*: Uploads the final processed data to the target database (Snowflake)

**Snowflake is optional.** Stages 1–4 write plain files to disk, and the
**`fixed_{table}.csv` files written by the Fix stage are the final PCORnet CDM
output**. Teams that only need the converted files, or that load them into
another database (Oracle, PostgreSQL, SQL Server, ...), can stop after `fix`.
See [Running without Snowflake (file output only)](#running-without-snowflake-file-output-only).

Additional pipelines are available for:
- **Analysis**: Generate monthly counts and distinct value reports
- **Data Quality Checks**: Run mapping gap reports, mapping reports, and data quality validations
- **Scheduler**: Automated batch processing of incoming data

# Requirements

This tool requires Docker to be installed. Ovid automatically provisions an ephemeral PySpark cluster using Docker Compose for each job execution.

## Supported Databases

**A database is not required to convert data.** The `upload` job is the only
ETL stage that connects to a database, and Snowflake is currently the only
supported upload target. Support for PostgreSQL and Microsoft SQL Server is
planned for future implementation. To use another database, run the ETL up to
`fix` and load the `fixed_*.csv` files yourself (see
[Running without Snowflake](#running-without-snowflake-file-output-only)).


# Installation

Before installing Ovid, you must configure the installation settings:

1. Copy the sample settings file:

        cp ovid_setup/install_settings_sample.py ovid_setup/install_settings.py

2. Edit `ovid_setup/install_settings.py` and configure the following:
   - **General**: Set `PYSPARK_WORKERS_COUNT`
   - **Paths**: Set `CREDENTIALS_PATH`, `OVID_INSTALL_PATH`, and `OMOP_CONCEPT_TABLES_PATH`
   - **Database settings**: Configure `DB_TYPE`, `DB_SERVER_NAME`
   - **Snowflake settings** (only needed for the `upload` job and the scheduler; leave the placeholders if you only need file output): Configure `SF_STAGE_NAME`, `SF_ROLE`, `SF_DB_USER`, `SF_WAREHOUSE`, `SF_PRIVATE_KEY_FILE_NAME`, and `SF_PRIVATE_KEY_PASSPHRASE_FILE_NAME`
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

## Target CDM version

The `pcornet_mappers` target **PCORnet CDM v7.0** (`CDM_VERSION = '070'`). The schema and valuesets
are read from `common/lists/2025_01_23_PCORnet_Common_Data_Model_v7dot0_parseable.xlsx` and
`common/cdm/pcornet_7_0/`.

## Partner templates

`ovid_setup/templates/` contains the starting point for a new partner:

| Template | Source data |
|----------|-------------|
| `omop_partner_plus` | OMOP CDM 5.x ("OMOP Plus": OMOP tables plus `*_sup` supplemental tables, `address.csv`, `hash_token*.csv`, ...) |
| `omop_partner_1`    | OneFL OMOP-like extract |
| `pcornet_partner_1` | Native PCORnet CDM files |

Each template ships the formatters, fixers and `dictionaries.py` required by every mapper in
`mapping_scripts/pcornet_mappers/`. All three parts belong to the same release, so update the
dictionaries, formatters and fixers together with `common/` and `mapping_scripts/`. To check a
template or your own partner folder against the mappers, run:

        python3 ovid_setup/validate_templates.py                       # all templates
        python3 ovid_setup/validate_templates.py partners/my_partner   # your partner folder

This reports any dictionary a mapper references that `dictionaries.py` does not define, and any
column a mapper reads that the formatter does not produce. The generated template dictionaries
hold generic PCORnet v7.0 valueset mappings. Extend them with your site's raw source values.

Notes for PCORnet v7.0 fields:
- `DEMOGRAPHIC.RACE_ETH_*`: the formatters emit the eight `RACE_ETH_*` columns empty, and the
  common `race_eth_fix` (enabled in `fixer_scripts/demographic_fixer.py`) derives them from `RACE` / `HISPANIC`.
- `LDS_ADDRESS_HISTORY.STATE_FIPS / COUNTY_FIPS / RUCA_ZIP`: derived by the mapper from
  `ADDRESS_ZIP9` / `ADDRESS_ZIP5` using the geocoding reference below.

## Geocoding reference file

`common/lists/FULL_GEOCODING_202603_16.csv.gz` (ZIP9 to Census state/county/tract/block-group and
RUCA-2020) is included in the repository, gzip-compressed because the uncompressed file is
about 430 MB. No extra step is needed: Ovid reads the `.csv.gz` directly, or a decompressed
`FULL_GEOCODING_202603_16.csv` if one is present. See `common/lists/README.md`.

# Running Ovid

Ovid uses a command-line interface with subcommands for different pipelines.

## Before running your scripts:

1. Change permissions on all the folders and files in the repository to 777 by going to the upper folder and running the following command:

        chmod -R 777 .


## Command Structure

        python3 ovid.py <command> [options]

Available commands:
- `etl` - Run ETL jobs (format, deduplicate, map, fix, upload)
- `analysis` - Run analysis jobs (format, monthly_counts, distinct_values, raw_distinct_values)
- `dq_checks` - Run data quality checks (format, mapping_gap, mapping_report, dc_checks)
- `scheduler` - Run the scheduler to process data batches
- `tools` - Run utility jobs (raw_upload)


## Default Parameters

Ovid uses a two-level default parameter system. If a parameter is not provided on the command line, Ovid will fall back to either the partner-specific settings or the global defaults, depending on the parameter (see Parameter Resolution Order below).

### Global Defaults (common/settings.py)

These defaults apply to all partners unless overridden:

| Parameter | Default Value | Description |
|-----------|---------------|-------------|
| `DEFAULT_WORKERS_COUNT` | 25 | Number of Spark workers |
| `DEFAULT_MASTER_MEMORY` | 8g | Memory allocated to Spark master |
| `DEFAULT_WORKER_MEMORY` | 16g | Memory allocated to each Spark worker |
| `DEFAULT_INPUT_DIRECTORY` | *(empty)* | Default input data directory (must be set via `-id` or configured in settings) |
| `DEFAULT_JOBS` | all | Default job(s) to run |
| `DEFAULT_TABLES` | all | Default table(s) to process |
| `DEFAULT_DB_TYPE` | sf | Default database type (Snowflake) |
| `DEFAULT_DB_SERVER` | *(empty)* | Default database server (must be set via `-s` or configured in settings) |

### Partner-Specific Defaults (partners/[partner_name]/partner_settings.py)

Each partner can override defaults in their own `partner_settings.py`:

| Parameter | Description |
|-----------|-------------|
| `DEFAULT_PARTNER_DB_NAME` | Default database name for this partner |
| `DEFAULT_UPLOAD_TYPE` | Upload type: `merge` or `no_merge` |
| `MERGE_DATABASE_NAME` | Database name for merge operations |
| `MERGE_SCHEMA_NAME` | Schema name for merge operations |

### Parameter Resolution Order

When running a command, parameters are resolved using a two-level system (highest priority first). The fallback level depends on the parameter:

**Parameters that fall back to global defaults** (`common/settings.py`):
- `input_directory`, `workers_count`, `jobs`, `tables`, `db_server`, `db_type`
- Resolution: **Command-line argument** > **Global default**

**Parameters that fall back to partner-specific settings** (`partners/[partner]/partner_settings.py`):
- `db_name`, `upload_type`, `merge_database`, `merge_schema`
- Resolution: **Command-line argument** > **Partner setting**

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


## Running without Snowflake (file output only)

Snowflake is only used by the `upload` job. If you only need the converted
PCORnet CDM files, or you plan to load them into another database (Oracle,
PostgreSQL, SQL Server, ...), run the four file-producing stages and leave out
`upload`:

        python3 ovid.py etl -p [partner_name] -f [folder_name] -t all -j format deduplicate map fix

                e.g.   python3 ovid.py etl -p partnerA -f q2_2023 -t all -j format deduplicate map fix

Do **not** use `-j all` for this: `all` includes `upload`. The `-s`, `-db`,
`-dt`, `-ut`, `-mdb` and `-ms` options are not needed.

### Final output: the `fixed_*` tables

The **final PCORnet CDM output is the set of `fixed_{table}.csv` files** written
by the Fix stage:

        partners/[partner_name]/data/[folder_name]/fixer_output/fixed_{table}.csv

                e.g.   partners/partnerA/data/q2_2023/fixer_output/fixed_demographic.csv
                       partners/partnerA/data/q2_2023/fixer_output/fixed_encounter.csv
                       partners/partnerA/data/q2_2023/fixer_output/fixed_diagnosis.csv

These are the same files the `upload` job would send to Snowflake (it uploads
every `fixed_*.csv` in `fixer_output/`). Use these files, and **not** the
intermediate `formatted_*`, `deduplicated_*` or `mapped_*` files, as the
deliverable. The intermediate outputs have not had the fixes applied yet
(orphan removal, date corrections, valueset enforcement, race/ethnicity
derivation, ...).

File format:

- **Tab-delimited** (`\t`), one header row with the PCORnet CDM column names,
  no quote characters.
- Missing values are empty fields.
- Patient and record identifiers (`PATID`, `ENCOUNTERID`, ...) are hashed by
  the mappers with the encryption seed (`SEED_FILE_NAME`).

When loading into another database, declare the delimiter as tab and treat
empty fields as `NULL`. For example, with Oracle SQL\*Loader use
`FIELDS TERMINATED BY X'09' TRAILING NULLCOLS` and skip the header row
(`OPTIONS (SKIP=1)`). Column names and types for each table are listed in the
PCORnet CDM v7.0 specification (`common/cdm/pcornet_7_0/pcornet_cdm_v70.csv`).

### Configuration when not using Snowflake

The installer still creates `common/ovid_secrets.py`, which Ovid imports at
startup. If you never run `upload`:

- In `ovid_setup/install_settings.py`, leave the `SF_*` / `DB_SERVER_NAME`
  values as placeholders. They are only read by the `upload` job, the
  scheduler, and `tools raw_upload`.
- You **do** need the encryption seed file (`SEED_FILE_NAME` in
  `CREDENTIALS_PATH`). The mappers use it to hash identifiers.
- Datavant settings are only needed for Datavant token matching.
- Avoid the common fixes that read the existing data from Snowflake (for
  example `append_to_existing_lds_address_hx_fix`). The partner templates do
  not enable them.
- The scheduler keeps its batch state in Snowflake. For a file-only setup, run
  `python3 ovid.py etl` directly.


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

**Note:** This feature is still in development. The command is accepted but does not currently execute any checks.


# Parameter Reference

## ETL Parameters

| Parameter | Description |
|-----------|-------------|
| `-p, --partner` | Partner or site name (required). Used to pull partner/site custom dictionaries (e.g., partnera) |
| `-f, --ovid_input_folder` | Input folder name(s) where raw data resides (required) |
| `-t, --table` | Table name(s) to run the job on (e.g., demographic, encounter, or all) |
| `-j, --job` | Job type: `all`, `format`, `deduplicate`, `map`, `fix`, `upload`. `all` includes `upload`; use `format deduplicate map fix` for file output only |
| `-s, --db_server` | Database server address or Snowflake account (`upload` only) |
| `-db, --db_name` | Database name for upload (`upload` only) |
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
| `-j, --job` | Job type: `all`, `format`, `monthly_counts`, `distinct_values`, `raw_distinct_values` |
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
        SCHEDULER_POLL_INTERVAL_IN_MINUTES = 1  # Interval of how long the scheduler will go to sleep in minutes
        SCHEDULER_DB_TYPE   = 'sf'     # Type of database. Currently only supporting Snowflake
        SCHEDULER_DB_SERVER = '[server_name]'  
        SCHEDULER_DB_NAME   = 'OVID'
        SCHEDULER_DB_SCHEMA    = 'SCHEDULER'
        MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES = 1  # The duration of how long the scheduler will wait for no modification to occur on a data set before running the process

2. Use the following command to run the scheduler:

        python3 ovid.py scheduler -id [path to main folder where the scheduler will be monitoring]

                e.g.   python3 ovid.py scheduler -id /ovid_data
