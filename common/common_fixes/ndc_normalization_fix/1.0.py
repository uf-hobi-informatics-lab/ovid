"""NDC normalization fix (v1.0).

Normalizes National Drug Code (NDC) values into the canonical PCORnet CDM
form: an 11-digit, hyphen-free numeric string. Raw NDC values frequently
arrive as hyphenated 5-4-2 / 5-3-2 / 4-4-2 segmented strings (e.g.
``17271-701-02``) or as under-padded numeric strings (e.g. ``6332320805``),
both of which PCORnet EDC flags as bad codes (``UNEXP_LENGTH`` /
``UNEXP_STRING``).

The normalization performed on the target column is:

1. Strip all hyphens.
2. Keep only the digits (any remaining non-digit characters make the value
   ineligible and it is left unchanged).
3. Left-zero-pad the resulting digit string to 11 characters when it is 11
   digits or fewer.

Parameters
----------
src1 : str
    The name of the NDC column to normalize (e.g. ``NDC``,
    ``RXNORM_CUI``, ``MEDADMIN_CODE``).
src2 : str
    Eligibility mode. One of:

    - ``all`` (or ``NotUsed``): every value that is composed solely of
      digits and hyphens and whose digit count is between 1 and 11 is
      normalized. Use this for columns that are guaranteed to be NDC codes
      (e.g. the DISPENSING ``NDC`` column).
    - ``hyphenated_only``: only values that actually contain a hyphen are
      normalized. Use this for mixed columns where a bare numeric could be a
      different code system that must not be zero-padded (e.g.
      ``RXNORM_CUI``, where short RxNorm CUIs like ``9997`` must be left
      untouched but segmented NDCs like ``99999-999-31`` should be
      normalized).

Behavior
--------
Values that are null, empty, that contain non-digit/non-hyphen characters,
or whose digit count exceeds 11, are left unchanged. Ineligible values under
``hyphenated_only`` (no hyphen present) are also left unchanged. Rows whose
target value actually changes are written to a diagnostics CSV (original
values, before normalization) before the corrected table is written back.
Rows are never deleted.
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, regexp_replace, length, lpad, instr
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
input_data_folder = args.data_folder
examined_table_name = args.table
input_partner_name = args.partner
ndc_field_name = args.src1
normalization_mode = (args.src2 or "all").strip().lower()
input_data_folder_path = args.input_path
output_data_folder_path = args.output_path
fixed_table_name = args.fixed_table_name

# Treat the standard placeholder as the default "all" mode.
if normalization_mode in ("notused", ""):
    normalization_mode = "all"

spark = cf.get_spark_session("ndc_normalization_fix")

this_fix_name = 'ndc_normalization_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')

try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=f"{this_fix_name} ({ndc_field_name}, mode={normalization_mode})"
    )

    # Read the current fixed table
    examined_table = cf.spark_read(output_data_folder_path + fixed_table_name, spark)

    # Digits-only representation of the NDC (hyphens and any other separators removed)
    digits_only = regexp_replace(col(ndc_field_name), "[^0-9]", "")

    # A value is structurally an NDC candidate when, after stripping non-digits,
    # it is a non-empty digit string of length <= 11 AND the original contained
    # no characters other than digits and hyphens.
    hyphen_free = regexp_replace(col(ndc_field_name), "-", "")
    is_only_digits_and_hyphens = hyphen_free == digits_only
    structural_condition = (
        col(ndc_field_name).isNotNull()
        & (length(digits_only) > lit(0))
        & (length(digits_only) <= lit(11))
        & is_only_digits_and_hyphens
    )

    if normalization_mode == "hyphenated_only":
        # Only touch values that actually contain a hyphen (segmented NDC form),
        # leaving bare numerics (e.g. RxNorm CUIs) untouched.
        eligible_condition = structural_condition & (instr(col(ndc_field_name), "-") > lit(0))
    else:
        eligible_condition = structural_condition

    normalized_value = lpad(digits_only, 11, "0")

    # Capture rows whose value will actually change (for diagnostics) -- before modification
    changed_rows = examined_table.filter(
        eligible_condition & (col(ndc_field_name) != normalized_value)
    )

    # Apply normalization only to eligible rows; everything else passes through unchanged
    fixed_table = examined_table.withColumn(
        ndc_field_name,
        when(eligible_condition, normalized_value).otherwise(col(ndc_field_name))
    )

    # Write diagnostics -- rows that were normalized (original values, before change)
    cf.write_pyspark_output_file(
        payspark_df=changed_rows,
        output_file_name=f"{examined_table_name.replace('.csv', '')}_ndc_normalized_{ndc_field_name}_.csv",
        output_data_folder_path=output_data_folder_path + "/fixers_output/"
                                + examined_table_name_parsed + "_fixer/" + this_fix_name + "/"
    )

    # Write the corrected table back
    cf.write_pyspark_output_file(
        payspark_df=fixed_table,
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
