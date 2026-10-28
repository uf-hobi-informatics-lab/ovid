"""Carry an Ovid-built address-history table forward from the previous cycle (v2.0).

For partners where Ovid *builds* ``LDS_ADDRESS_HISTORY`` /
``PRIVATE_ADDRESS_HISTORY`` (e.g. from the demographic ZIP joined to
encounters) and the partner submits a full refresh (``no_merge``), each cycle
rebuilds the table from scratch, so address history and patients that drop out
of the current build are lost and the row count can shrink cycle-over-cycle.

This fix reads the previous cycle's table from Snowflake and reconciles the
freshly fixed table against it with
``CommonFuncitons.merge_address_history_with_zip_transitions`` (same-ZIP rows
are continued, moved-away rows are closed with ``CURRENT_ADDRESS_FLAG='N'``,
patients missing from this build are carried forward, one current row per
PATID). Run it BEFORE ``patid_orphans_fix`` so carried-forward rows for
patients no longer in DEMOGRAPHIC are removed.

Supersedes v1.0 (hardcoded ``PUBLIC_18R2`` schema, PATID-only joins with
fan-out, bare excepts, no CURRENT_ADDRESS_FLAG maintenance). v1.0 is kept for
rollback.

Parameters
----------
src1 : str
    Snowflake account/server (e.g. ``your-account.region``). ``NotUsed``
    or empty -> ``settings.DEFAULT_DB_SERVER``.
src2 : str
    Previous-cycle table, ``DB.SCHEMA.TABLE`` or ``DB.SCHEMA`` (table name is
    then derived from the fixed table, e.g. ``LDS_ADDRESS_HISTORY``).
    ``NotUsed`` or empty -> the partner's ``MERGE_DATABASE_NAME`` /
    ``MERGE_SCHEMA_NAME``.

Failure mode: any error (Snowflake read, column mismatch, ...) is reported via
``print_failure_message`` so ``apply_fix`` quarantines the table and fails the
fix stage loudly -- a silent pass-through is exactly the regression this fix
exists to prevent.
"""

import argparse
import importlib
from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, date_format, lit
from pyspark.sql.types import DateType, StringType, TimestampType

from commonFunctions import CommonFuncitons
from ovid_logging import get_logger
import settings


THIS_FIX_NAME = 'append_to_existing_lds_address_hx_fix_2.0'
UNUSED_PARAMETER_VALUES = ('', 'NOTUSED', 'NONE')

logger = get_logger()


def parse_arguments() -> argparse.Namespace:
    """Parse the standard common-fix CLI arguments passed by ``apply_fix``.

    Returns:
        The parsed argument namespace.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("-f", "--data_folder")
    parser.add_argument("-t", "--table")
    parser.add_argument("-p", "--partner")
    parser.add_argument("-ftm", "--fixed_table_name")
    parser.add_argument("-i", "--input_path")
    parser.add_argument("-o", "--output_path")
    parser.add_argument("-sr1", "--src1")
    parser.add_argument("-sr2", "--src2")
    return parser.parse_args()


def is_unused(value: Optional[str]) -> bool:
    """Return True when a src parameter is empty or the ``NotUsed`` sentinel.

    Args:
        value: Raw src1/src2 value.

    Returns:
        True if the value should be treated as not provided.
    """
    return value is None or value.strip().upper() in UNUSED_PARAMETER_VALUES


def resolve_existing_table(
    partner_name: str, fixed_table_name: str, table_location: Optional[str]
) -> tuple[str, str, str]:
    """Resolve the fully qualified previous-cycle table to merge against.

    Args:
        partner_name: Partner name (used to load ``partner_settings``).
        fixed_table_name: Fixed CSV name, e.g. ``fixed_lds_address_history.csv``.
        table_location: src2 value: ``DB.SCHEMA.TABLE``, ``DB.SCHEMA`` or unused.

    Returns:
        ``(database, schema, table)`` names.

    Raises:
        ValueError: If ``table_location`` is not ``DB.SCHEMA`` or ``DB.SCHEMA.TABLE``.
    """
    default_table = fixed_table_name.replace('fixed_', '').replace('.csv', '').upper()

    if is_unused(table_location):
        partner_settings = importlib.import_module(f"partners.{partner_name.lower()}.partner_settings")
        return partner_settings.MERGE_DATABASE_NAME, partner_settings.MERGE_SCHEMA_NAME, default_table

    location_parts = [part.strip() for part in table_location.split('.')]
    if len(location_parts) == 2:
        return location_parts[0], location_parts[1], default_table
    if len(location_parts) == 3:
        return location_parts[0], location_parts[1], location_parts[2]
    raise ValueError(
        f"src2 must be DB.SCHEMA or DB.SCHEMA.TABLE, got '{table_location}'"
    )


def align_existing_to_incoming(existing_df: DataFrame, incoming_df: DataFrame) -> DataFrame:
    """Project the Snowflake table onto the incoming CSV columns, all as strings.

    Date/timestamp columns are rendered as ``yyyy-MM-dd`` / ``yyyy-MM-dd HH:mm:ss``
    to match the tab-delimited CSV convention. Incoming columns missing from the
    existing table are added as NULL; extra existing columns are dropped.

    Args:
        existing_df: Previous-cycle table read from Snowflake.
        incoming_df: Fixed CSV for this batch (all string columns).

    Returns:
        ``existing_df`` with exactly ``incoming_df``'s columns, in order.
    """
    existing_types = {field.name.upper(): field for field in existing_df.schema.fields}
    missing_columns = [name for name in incoming_df.columns if name.upper() not in existing_types]
    dropped_columns = [
        field.name for field in existing_df.schema.fields
        if field.name.upper() not in {name.upper() for name in incoming_df.columns}
    ]
    if missing_columns or dropped_columns:
        logger.warning(
            "%s: column differences vs existing table; added as NULL=%s, dropped=%s",
            THIS_FIX_NAME, missing_columns, dropped_columns,
        )

    projected_columns = []
    for column_name in incoming_df.columns:
        existing_field = existing_types.get(column_name.upper())
        if existing_field is None:
            projected_columns.append(lit(None).cast(StringType()).alias(column_name))
        elif isinstance(existing_field.dataType, DateType):
            projected_columns.append(
                date_format(col(existing_field.name), "yyyy-MM-dd").alias(column_name)
            )
        elif isinstance(existing_field.dataType, TimestampType):
            projected_columns.append(
                date_format(col(existing_field.name), "yyyy-MM-dd HH:mm:ss").alias(column_name)
            )
        else:
            projected_columns.append(col(existing_field.name).cast(StringType()).alias(column_name))
    return existing_df.select(*projected_columns)


def main() -> None:
    """Run the fix: read, reconcile against the previous cycle, write back.

    Returns:
        None.
    """
    args = parse_arguments()
    common_functions = CommonFuncitons('NotUsed')
    partner_name: str = args.partner
    output_data_folder_path: str = args.output_path
    fixed_table_name: str = args.fixed_table_name
    examined_table_name_parsed = args.table.replace('mapped_', '').replace('.csv', '')

    spark = common_functions.get_spark_session(THIS_FIX_NAME)

    try:
        common_functions.print_fixer_status(
            current_count='**', total_count='**', fix_type='common', fix_name=THIS_FIX_NAME,
        )

        db_server = settings.DEFAULT_DB_SERVER if is_unused(args.src1) else args.src1.strip()
        database_name, schema_name, existing_table_name = resolve_existing_table(
            partner_name, fixed_table_name, args.src2
        )
        qualified_table = f"{database_name}.{schema_name}.{existing_table_name}"

        incoming_df = spark.read.load(
            output_data_folder_path + fixed_table_name,
            format="csv", sep="\t", inferSchema="false", header="true", quote='"',
        )

        existing_raw_df = common_functions.read_from_snowflake(
            db_server=db_server,
            db_name=database_name,
            schema_name=schema_name,
            table_name=qualified_table,
        )
        existing_df = align_existing_to_incoming(existing_raw_df, incoming_df).cache()
        incoming_df = incoming_df.cache()

        incoming_count = incoming_df.count()
        existing_count = existing_df.count()

        reconciled_df = CommonFuncitons.merge_address_history_with_zip_transitions(
            incoming_df=incoming_df,
            existing_df=existing_df,
            enforce_single_current=True,
        )

        # write_pyspark_output_file runs the Spark job into a tmp_ directory
        # first and only then replaces the CSV, so overwriting the file the
        # incoming rows were read from is safe.
        common_functions.write_pyspark_output_file(
            payspark_df=reconciled_df,
            output_file_name=fixed_table_name,
            output_data_folder_path=output_data_folder_path,
        )
        # Drop the cached incoming frame first: it has the same path/plan as the
        # re-read below and Spark's CacheManager would otherwise return it.
        incoming_df.unpersist()
        existing_df.unpersist()
        output_count = spark.read.load(
            output_data_folder_path + fixed_table_name,
            format="csv", sep="\t", inferSchema="false", header="true", quote='"',
        ).count()

        diagnostics_path = (
            output_data_folder_path + "fixers_output/" + examined_table_name_parsed
            + "_fixer/" + THIS_FIX_NAME + "/"
        )
        summary_df = spark.createDataFrame(
            [(qualified_table, str(existing_count), str(incoming_count), str(output_count))],
            ["EXISTING_TABLE", "EXISTING_ROWS", "INCOMING_ROWS", "OUTPUT_ROWS"],
        )
        common_functions.write_pyspark_output_file(
            payspark_df=summary_df,
            output_file_name=examined_table_name_parsed + "_carry_forward_summary.csv",
            output_data_folder_path=diagnostics_path,
        )

        logger.info(
            "%s: partner=%s table=%s existing=%s (%d rows) incoming=%d output=%d",
            THIS_FIX_NAME, partner_name, fixed_table_name, qualified_table,
            existing_count, incoming_count, output_count,
        )
        spark.stop()

    except Exception as error:
        logger.error(
            "%s failed for partner=%s table=%s", THIS_FIX_NAME, partner_name, fixed_table_name,
            exc_info=True,
        )
        spark.stop()
        common_functions.print_failure_message(
            folder=args.data_folder,
            partner=partner_name.lower(),
            job=THIS_FIX_NAME,
            text=str(error),
        )


if __name__ == "__main__":
    main()
