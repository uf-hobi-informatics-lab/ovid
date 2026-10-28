"""Backfill PRIVATE_ADDRESS_HISTORY from the reconciled LDS_ADDRESS_HISTORY (v1.0).

For partners where Ovid builds both address tables from the same source (e.g.
demographic ZIP joined to encounters), ``LDS_ADDRESS_HISTORY`` and
``PRIVATE_ADDRESS_HISTORY`` should describe the same addresses. When the
previous-cycle PRIVATE table is thinner than the previous-cycle LDS table, carrying each table
forward separately leaves PRIVATE missing patients and addresses.

This fix adds a PRIVATE row for every fixed-LDS row whose (PATID, ADDRESS_ZIP5)
is absent from the fixed PRIVATE table. Shared columns (including ADDRESSID,
period dates, flag, FIPS/RUCA, UPDATED, SOURCE) are copied from LDS;
PRIVATE-only columns (ADDRESS_STREET, ADDRESS_DETAIL, RAW_ADDRESS_TEXT, ...)
are NULL. It then keeps at most one ``CURRENT_ADDRESS_FLAG='Y'`` row per
PATID (latest start, open/latest end, existing PRIVATE row before backfilled,
highest ADDRESSID). Backfilled rows whose ADDRESSID already exists in PRIVATE
are skipped (primary key safety) and counted in the diagnostics.

Run it AFTER both the LDS and PRIVATE address-history fixers, i.e. from the
partner's ``multi_tables_fixer.py`` (``input_path`` = ``output_path`` = the
fixer output folder).

Parameters
----------
src1 : str
    Fixed LDS file name in the fixer output folder; ``NotUsed`` ->
    ``fixed_lds_address_history.csv``.
src2 : str
    Not used.
"""

import argparse
import os

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.functions import coalesce, col, lit, when
from pyspark.sql.types import StringType

from commonFunctions import CommonFuncitons
from ovid_logging import get_logger


THIS_FIX_NAME = 'sync_private_address_from_lds_fix_1.0'
DEFAULT_LDS_FILE_NAME = 'fixed_lds_address_history.csv'
NULL_ZIP_KEY = '\u0000'

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


def with_zip_key(data_frame: DataFrame) -> DataFrame:
    """Add a NULL-safe ``_zip_key`` column built from ``ADDRESS_ZIP5``.

    Args:
        data_frame: Address-history DataFrame.

    Returns:
        ``data_frame`` with the extra ``_zip_key`` column.
    """
    return data_frame.withColumn("_zip_key", coalesce(col("ADDRESS_ZIP5"), lit(NULL_ZIP_KEY)))


def build_backfill_rows(lds_df: DataFrame, private_df: DataFrame) -> DataFrame:
    """Project LDS rows missing from PRIVATE onto the PRIVATE column layout.

    Args:
        lds_df: Fixed LDS_ADDRESS_HISTORY (string columns).
        private_df: Fixed PRIVATE_ADDRESS_HISTORY (string columns).

    Returns:
        Backfill rows with exactly ``private_df``'s columns (no ADDRESSID that
        already exists in ``private_df``).
    """
    private_keys = with_zip_key(private_df).select("PATID", "_zip_key").distinct()
    missing_lds_rows = with_zip_key(lds_df).join(private_keys, on=["PATID", "_zip_key"], how="left_anti")
    missing_lds_rows = missing_lds_rows.join(
        private_df.select("ADDRESSID").distinct(), on="ADDRESSID", how="left_anti"
    )
    lds_columns = set(lds_df.columns)
    return missing_lds_rows.select(
        *[
            col(column_name) if column_name in lds_columns
            else lit(None).cast(StringType()).alias(column_name)
            for column_name in private_df.columns
        ]
    )


def enforce_single_current(address_df: DataFrame, output_columns: list[str]) -> DataFrame:
    """Keep at most one ``CURRENT_ADDRESS_FLAG='Y'`` row per PATID.

    Args:
        address_df: Address rows with a ``_from_backfill`` (0/1) column.
        output_columns: Columns to return, in order.

    Returns:
        DataFrame with surplus ``Y`` rows flipped to ``N``.
    """
    is_flag_y = F.upper(coalesce(col("CURRENT_ADDRESS_FLAG"), lit(""))) == lit("Y")
    rank_window = Window.partitionBy("PATID", "_is_y").orderBy(
        F.to_date(col("ADDRESS_PERIOD_START"), "yyyy-MM-dd").desc_nulls_last(),
        F.to_date(col("ADDRESS_PERIOD_END"), "yyyy-MM-dd").desc_nulls_first(),
        col("_from_backfill").asc(),
        col("ADDRESSID").desc_nulls_last(),
    )
    return (
        address_df
        .withColumn("_is_y", is_flag_y)
        .withColumn("_rank", F.row_number().over(rank_window))
        .withColumn(
            "CURRENT_ADDRESS_FLAG",
            when(col("_is_y") & (col("_rank") > 1), lit("N")).otherwise(col("CURRENT_ADDRESS_FLAG")),
        )
        .select(*output_columns)
    )


def main() -> None:
    """Run the fix: backfill PRIVATE from LDS and write it back.

    Returns:
        None.
    """
    args = parse_arguments()
    common_functions = CommonFuncitons('NotUsed')
    partner_name: str = args.partner
    output_data_folder_path: str = args.output_path
    fixed_table_name: str = args.fixed_table_name
    lds_file_name = (
        DEFAULT_LDS_FILE_NAME
        if args.src1 is None or args.src1.strip().upper() in ('', 'NOTUSED')
        else args.src1.strip()
    )

    spark = common_functions.get_spark_session(THIS_FIX_NAME)

    try:
        common_functions.print_fixer_status(
            current_count='**', total_count='**', fix_type='common', fix_name=THIS_FIX_NAME,
        )

        private_path = output_data_folder_path + fixed_table_name
        lds_path = output_data_folder_path + lds_file_name
        if not os.path.exists(private_path) or not os.path.exists(lds_path):
            common_functions.print_with_style(
                f"{THIS_FIX_NAME}: {private_path} or {lds_path} missing for "
                f"{partner_name.lower()}; skipping.",
                'warning orange',
            )
            spark.stop()
            return

        csv_options = dict(format="csv", sep="\t", inferSchema="false", header="true", quote='"')
        private_df = spark.read.load(private_path, **csv_options).cache()
        lds_df = spark.read.load(lds_path, **csv_options)

        private_count = private_df.count()
        backfill_df = build_backfill_rows(lds_df, private_df).cache()
        backfill_count = backfill_df.count()

        combined_df = private_df.withColumn("_from_backfill", lit(0)).unionByName(
            backfill_df.withColumn("_from_backfill", lit(1))
        )
        synced_df = enforce_single_current(combined_df, private_df.columns)

        # write_pyspark_output_file materialises into a tmp_ directory before
        # replacing the CSV, so overwriting the file read above is safe.
        common_functions.write_pyspark_output_file(
            payspark_df=synced_df,
            output_file_name=fixed_table_name,
            output_data_folder_path=output_data_folder_path,
        )
        private_df.unpersist()
        backfill_df.unpersist()
        output_count = spark.read.load(private_path, **csv_options).count()

        table_name_parsed = fixed_table_name.replace('fixed_', '').replace('.csv', '')
        summary_df = spark.createDataFrame(
            [(str(private_count), str(backfill_count), str(output_count))],
            ["PRIVATE_ROWS_BEFORE", "BACKFILLED_FROM_LDS", "PRIVATE_ROWS_AFTER"],
        )
        common_functions.write_pyspark_output_file(
            payspark_df=summary_df,
            output_file_name=table_name_parsed + "_sync_from_lds_summary.csv",
            output_data_folder_path=(
                output_data_folder_path + "fixers_output/" + table_name_parsed
                + "_fixer/" + THIS_FIX_NAME + "/"
            ),
        )

        logger.info(
            "%s: partner=%s private before=%d backfilled=%d after=%d",
            THIS_FIX_NAME, partner_name, private_count, backfill_count, output_count,
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
