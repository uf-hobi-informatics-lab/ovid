"""Illogical discharge-date fix (v1.0).

Caps a date field when it exceeds the encounter's DISCHARGE_DATE by more
than 5 days.  Since DISCHARGE_DATE is not part of most CDM child tables,
the fix joins the encounter table (specified via ``src2``) on ENCOUNTERID
to obtain it.

Parameters
----------
src1 : str
    The date column to examine (e.g. ``PX_DATE``).
src2 : str
    The encounter file name to join for DISCHARGE_DATE
    (e.g. ``mapped_encounter.csv``).

Rows where ``datediff(to_date(src1), to_date(DISCHARGE_DATE)) > 5`` have
the date field replaced with DISCHARGE_DATE.  Rows are preserved (not
deleted).  The original rows (before replacement) are written to a
diagnostics file for audit.  Rows with null DISCHARGE_DATE or null
ENCOUNTERID are left unchanged -- no comparison is possible.
"""

from pyspark.sql.functions import col, datediff, to_date, when
from commonFunctions import CommonFuncitons
import argparse


cf = CommonFuncitons('NotUsed')

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-t", "--table")
parser.add_argument("-p", "--partner")
parser.add_argument("-ftm", "--fixed_table_name")
parser.add_argument("-i", "--input_path")
parser.add_argument("-o", "--output_path")
parser.add_argument("-sr1", "--src1")
parser.add_argument("-sr2", "--src2")

args = parser.parse_args()
input_data_folder: str = args.data_folder
examined_table_name: str = args.table
input_partner_name: str = args.partner
field_to_examine_name: str = args.src1
encounter_file_name: str = args.src2
input_data_folder_path: str = args.input_path
output_data_folder_path: str = args.output_path
fixed_table_name: str = args.fixed_table_name

spark = cf.get_spark_session("illogical_discharge_date_more_than_5_days_fix")

this_fix_name = 'illogical_discharge_date_more_than_5_days_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')

# Temporary column name used during the join — dropped before output.
_DISCHARGE_DATE_LOOKUP = '_DISCHARGE_DATE_LOOKUP'

try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=f"{this_fix_name} ({field_to_examine_name})"
    )

    examined_table = cf.spark_read(
        output_data_folder_path + fixed_table_name, spark
    )
    original_columns = examined_table.columns

    # ------------------------------------------------------------------
    # Read the encounter table to obtain DISCHARGE_DATE per ENCOUNTERID.
    # Deduplicate on ENCOUNTERID in case of duplicates.
    # ------------------------------------------------------------------
    encounter_table = cf.spark_read(
        input_data_folder_path + encounter_file_name, spark
    )
    encounter_lookup = (
        encounter_table
        .select('ENCOUNTERID', 'DISCHARGE_DATE')
        .dropDuplicates(['ENCOUNTERID'])
        .withColumnRenamed('DISCHARGE_DATE', _DISCHARGE_DATE_LOOKUP)
    )

    joined_df = examined_table.join(
        encounter_lookup, on='ENCOUNTERID', how='left'
    )

    # ------------------------------------------------------------------
    # Condition: the examined date field is more than 5 days AFTER the
    # encounter's DISCHARGE_DATE.
    #   datediff(to_date(field), to_date(DISCHARGE_DATE)) > 5
    # PySpark datediff(a, b) = a - b in days, so positive means a > b.
    # ------------------------------------------------------------------
    illogical_condition = (
        col(_DISCHARGE_DATE_LOOKUP).isNotNull()
        & col(field_to_examine_name).isNotNull()
        & (datediff(
            to_date(col(field_to_examine_name)),
            to_date(col(_DISCHARGE_DATE_LOOKUP))
        ) > 5)
    )

    # Capture bad rows BEFORE replacement for diagnostics.
    bad_rows = (
        joined_df
        .filter(illogical_condition)
        .select(original_columns)
    )

    # Replace the date field with DISCHARGE_DATE where illogical.
    fixed_df = (
        joined_df
        .withColumn(
            field_to_examine_name,
            when(illogical_condition, col(_DISCHARGE_DATE_LOOKUP))
            .otherwise(col(field_to_examine_name))
        )
        .select(original_columns)
    )

    # Write diagnostics — rows that had illogical dates (original values).
    cf.write_pyspark_output_file(
        payspark_df=bad_rows,
        output_file_name=(
            f"{examined_table_name.replace('.csv', '')}"
            f"_illogical_{field_to_examine_name}_after_discharge_.csv"
        ),
        output_data_folder_path=(
            output_data_folder_path + "/fixers_output/"
            + examined_table_name_parsed + "_fixer/"
            + this_fix_name + "/"
        )
    )

    # Write the corrected table back.
    cf.write_pyspark_output_file(
        payspark_df=fixed_df,
        output_file_name=fixed_table_name,
        output_data_folder_path=output_data_folder_path
    )

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
        folder=input_data_folder,
        partner=input_partner_name.lower(),
        job=this_fix_name,
        text=str(e)
    )
