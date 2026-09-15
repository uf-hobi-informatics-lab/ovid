"""Delete when null or flavor-of-null fix (v1.0).

Removes rows where a specified field is null, empty, or contains a PCORnet
CDM "flavor of null" sentinel value (NI, UN, OT).  These sentinel values
indicate that the source system could not provide a meaningful value:

- NI = No information
- UN = Unknown
- OT = Other

Removed rows are written to a diagnostics file for audit.

Parameters
----------
src1 : str
    The name of the column to examine (e.g. ``MEDADMIN_TYPE``).
src2 : str
    Not used.  Pass ``"NotUsed"``.
"""

from pyspark.sql.functions import col, trim
from commonFunctions import CommonFuncitons
import argparse


FLAVOR_OF_NULL_VALUES = {"NI", "UN", "OT"}

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
input_data_folder = args.data_folder
examined_table_name = args.table
input_partner_name = args.partner
field_to_examine_name = args.src1
input_data_folder_path = args.input_path
output_data_folder_path = args.output_path
fixed_table_name = args.fixed_table_name

spark = cf.get_spark_session("delete_when_null_or_flavor_of_null")

this_fix_name = 'delete_when_null_or_flavor_of_null_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')

try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=f"{this_fix_name} ({field_to_examine_name})"
    )

    examined_table = cf.spark_read(output_data_folder_path + fixed_table_name, spark)

    # A row is "bad" if the field is null, empty/whitespace-only, or a
    # flavor-of-null sentinel (NI, UN, OT — exact uppercase match).
    bad_condition = (
        col(field_to_examine_name).isNull()
        | (trim(col(field_to_examine_name)) == "")
        | col(field_to_examine_name).isin(*FLAVOR_OF_NULL_VALUES)
    )

    good_rows = examined_table.filter(~bad_condition).cache()
    bad_rows = examined_table.filter(bad_condition).cache()

    # Materialize both DataFrames before any writes.  Because
    # write_pyspark_output_file() overwrites the source CSV via an awk
    # merge, the second Spark action would otherwise try to re-read a
    # file whose contents (and size) have changed, causing an
    # EOFException ("Cannot seek after EOF").  Caching breaks the lazy
    # lineage so both DataFrames are fully in memory before the first
    # write touches the source file.
    good_rows.count()
    bad_rows.count()

    # Overwrite the fixed table with only the good rows
    cf.write_pyspark_output_file(
        payspark_df=good_rows,
        output_file_name=fixed_table_name,
        output_data_folder_path=output_data_folder_path
    )

    # Write removed rows to diagnostics for audit
    cf.write_pyspark_output_file(
        payspark_df=bad_rows,
        output_file_name=f"{examined_table_name.replace('.csv', '')}_null_or_fon_{field_to_examine_name}_.csv",
        output_data_folder_path=output_data_folder_path + "/fixers_output/"
                                + examined_table_name_parsed + "_fixer/" + this_fix_name + "/"
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
