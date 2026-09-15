"""Delete records after death fix (v1.0).

Removes rows where a specified date field is after the patient's DEATH_DATE.
The fix joins the DEATH table on PATID to obtain DEATH_DATE.  Patients
without a DEATH record (null DEATH_DATE after the LEFT JOIN) are left
untouched — no comparison is possible.

This addresses DC_2_03 AFTER_DEATH illogical date flags.

Parameters
----------
src1 : str
    The date column to examine (e.g. ``EXT_RECORD_DATE``,
    ``VX_RECORD_DATE``).
src2 : str
    The death file name to join for DEATH_DATE
    (e.g. ``mapped_death.csv``).

Removed rows are written to a diagnostics file for audit.
"""

from pyspark.sql.functions import col, to_date
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
death_file_name: str = args.src2
input_data_folder_path: str = args.input_path
output_data_folder_path: str = args.output_path
fixed_table_name: str = args.fixed_table_name

spark = cf.get_spark_session("delete_records_after_death_fix")

this_fix_name = 'delete_records_after_death_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')

# Temporary column name used during the join — dropped before output.
_DEATH_DATE_LOOKUP = '_DEATH_DATE_LOOKUP'

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
    # Read the death table to obtain DEATH_DATE per PATID.
    # Deduplicate on PATID in case of multiple death records.
    # ------------------------------------------------------------------
    death_table = cf.spark_read(
        input_data_folder_path + death_file_name, spark
    )
    death_lookup = (
        death_table
        .select('PATID', 'DEATH_DATE')
        .dropDuplicates(['PATID'])
        .withColumnRenamed('DEATH_DATE', _DEATH_DATE_LOOKUP)
    )

    joined_df = examined_table.join(
        death_lookup, on='PATID', how='left'
    )

    # ------------------------------------------------------------------
    # Condition: the examined date field is AFTER the patient's
    # DEATH_DATE.  Only applies to patients who have a DEATH record
    # (non-null DEATH_DATE) and a non-null date field.
    # ------------------------------------------------------------------
    after_death_condition = (
        col(_DEATH_DATE_LOOKUP).isNotNull()
        & col(field_to_examine_name).isNotNull()
        & (to_date(col(field_to_examine_name)) > to_date(col(_DEATH_DATE_LOOKUP)))
    )

    good_rows = joined_df.filter(~after_death_condition).select(original_columns).cache()
    bad_rows = joined_df.filter(after_death_condition).select(original_columns).cache()

    # Materialize both DataFrames before any writes.  Because
    # write_pyspark_output_file() overwrites the source CSV via an awk
    # merge, the second Spark action would otherwise try to re-read a
    # file whose contents have changed, causing an EOFException.
    good_rows.count()
    bad_rows.count()

    # Overwrite the fixed table with only the good rows.
    cf.write_pyspark_output_file(
        payspark_df=good_rows,
        output_file_name=fixed_table_name,
        output_data_folder_path=output_data_folder_path
    )

    # Write removed rows to diagnostics for audit.
    cf.write_pyspark_output_file(
        payspark_df=bad_rows,
        output_file_name=(
            f"{examined_table_name.replace('.csv', '')}"
            f"_after_death_{field_to_examine_name}_.csv"
        ),
        output_data_folder_path=(
            output_data_folder_path + "/fixers_output/"
            + examined_table_name_parsed + "_fixer/"
            + this_fix_name + "/"
        )
    )

    good_rows.unpersist()
    bad_rows.unpersist()

    spark.stop()

except Exception as e:

    spark.stop()
    cf.print_failure_message(
        folder=input_data_folder,
        partner=input_partner_name.lower(),
        job=this_fix_name,
        text=str(e)
    )
