"""FIPS / RUCA normalization fix (DC_1_20 / DC_1_21).

Normalizes the geographic identifier columns in the LDS_ADDRESS_HISTORY and
PRIVATE_ADDRESS_HISTORY tables so they conform to the PCORnet CDM v7.0 format
expectations checked by DC 1.21 ("STATE_FIPS, COUNTY_FIPS and RUCA_ZIP fields
have values that do not conform to the expected content").

DC 1.21 fails when any of the following are true:
  * STATE_FIPS is not a 2-digit numeric string.
  * COUNTY_FIPS is not a 5-digit numeric string.
  * The first 2 digits of COUNTY_FIPS do not match STATE_FIPS.
  * RUCA_ZIP is not a numeric value that fits the CDM ``Text(2)`` width.

The shared ``pcornet_mappers`` populate these columns from the geocoding
reference table without zero-padding, so single-digit state FIPS values (e.g.
Alabama = "1") and short county FIPS values flow through un-padded. This fix
repairs the padding *after* mapping, scoped to the partner that wires it (so
the shared mapper is left untouched and other partners are unaffected).

Behavior
--------
* Reads the current fixed table at ``output_path + fixed_table_name``.
* For STATE_FIPS: keeps only digits, left-pads to 2 characters. Blank / all
  non-digit values are left as-is (governed by the missing-value checks, not
  DC 1.21).
* For COUNTY_FIPS: keeps only digits, left-pads to 5 characters. When both
  STATE_FIPS and COUNTY_FIPS are present, the leading 2 digits of COUNTY_FIPS
  are forced to equal the padded STATE_FIPS so the cross-field consistency
  rule holds.
* For RUCA_ZIP: keeps only the leading integer portion (strips any trailing
  ``.0`` float artifact from the reference spreadsheet) and blanks values that
  still exceed the 2-character CDM width.
* Writes a diagnostics CSV containing the pre-modification state of every row
  whose STATE_FIPS, COUNTY_FIPS or RUCA_ZIP value changed.
* Writes the corrected table back over ``fixed_table_name``.

Parameters
----------
src1 : str
    Comma-separated list of the three target column names, in the order
    ``STATE_FIPS,COUNTY_FIPS,RUCA_ZIP``. A subset may be supplied (e.g. only
    the two columns a table actually carries); any name not present in the
    table is skipped.
src2 : str
    Unused (pass ``NotUsed``).
"""

import argparse

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, concat, lit, lpad, length, regexp_extract, regexp_replace, substring, when
from commonFunctions import CommonFuncitons


STATE_FIPS_WIDTH: int = 2
COUNTY_FIPS_WIDTH: int = 5
RUCA_ZIP_MAX_WIDTH: int = 2

cf: CommonFuncitons = CommonFuncitons('NotUsed')

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-t", "--table")
parser.add_argument("-p", "--partner")
parser.add_argument("-ftm", "--fixed_table_name")
parser.add_argument("-i", "--input_path")
parser.add_argument("-o", "--output_path")
parser.add_argument("-sr1", "--src1")  # 'STATE_FIPS,COUNTY_FIPS,RUCA_ZIP'
parser.add_argument("-sr2", "--src2")  # NotUsed

args = parser.parse_args()
input_data_folder: str = args.data_folder
examined_table_name: str = args.table
input_partner_name: str = args.partner
target_columns_raw: str = (args.src1 or "").strip()
input_data_folder_path: str = args.input_path
output_data_folder_path: str = args.output_path
fixed_table_name: str = args.fixed_table_name

spark: SparkSession = cf.get_spark_session("fips_ruca_normalization_fix")

this_fix_name: str = 'common_fips_ruca_normalization_fix_1.0'
examined_table_name_parsed: str = examined_table_name.replace('mapped_', '').replace('.csv', '')

try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=f"{this_fix_name} ({target_columns_raw})"
    )

    requested_columns: list[str] = [c.strip() for c in target_columns_raw.split(',') if c.strip()]

    examined_table: DataFrame = spark.read.load(
        output_data_folder_path + fixed_table_name,
        format="csv", sep="\t", inferSchema="false", header="true", quote='"'
    )

    present_columns: list[str] = examined_table.columns
    has_state: bool = 'STATE_FIPS' in requested_columns and 'STATE_FIPS' in present_columns
    has_county: bool = 'COUNTY_FIPS' in requested_columns and 'COUNTY_FIPS' in present_columns
    has_ruca: bool = 'RUCA_ZIP' in requested_columns and 'RUCA_ZIP' in present_columns

    fixed_table: DataFrame = examined_table

    # STATE_FIPS -> 2-digit zero-padded numeric (blank left untouched).
    if has_state:
        state_digits = regexp_replace(col('STATE_FIPS'), r"[^0-9]", "")
        fixed_table = fixed_table.withColumn(
            'STATE_FIPS',
            when(length(state_digits) > 0, lpad(state_digits, STATE_FIPS_WIDTH, "0"))
            .otherwise(col('STATE_FIPS'))
        )

    # COUNTY_FIPS -> 5-digit zero-padded numeric, with leading 2 digits forced
    # to equal the (already normalized) STATE_FIPS when both are present.
    if has_county:
        county_digits = regexp_replace(col('COUNTY_FIPS'), r"[^0-9]", "")
        county_padded = lpad(county_digits, COUNTY_FIPS_WIDTH, "0")
        if has_state:
            county_padded = when(
                (length(regexp_replace(col('STATE_FIPS'), r"[^0-9]", "")) > 0)
                & (length(county_digits) > 0),
                concat(
                    lpad(regexp_replace(col('STATE_FIPS'), r"[^0-9]", ""), STATE_FIPS_WIDTH, "0"),
                    substring(county_padded, 3, COUNTY_FIPS_WIDTH - STATE_FIPS_WIDTH)
                )
            ).otherwise(county_padded)
        fixed_table = fixed_table.withColumn(
            'COUNTY_FIPS',
            when(length(county_digits) > 0, county_padded).otherwise(col('COUNTY_FIPS'))
        )

    # RUCA_ZIP -> leading integer portion; blank out anything still too wide.
    if has_ruca:
        ruca_int = regexp_extract(col('RUCA_ZIP'), r"^\s*([0-9]+)", 1)
        fixed_table = fixed_table.withColumn(
            'RUCA_ZIP',
            when(length(ruca_int) == 0, col('RUCA_ZIP'))
            .when(length(ruca_int) <= RUCA_ZIP_MAX_WIDTH, ruca_int)
            .otherwise(lit(""))
        )

    # Diagnostics: capture the pre-modification state of every row whose
    # STATE_FIPS, COUNTY_FIPS or RUCA_ZIP value would change. We re-derive the
    # normalized values inline against the original table (avoids a self-join).
    original: DataFrame = examined_table
    change_mask = lit(False)

    if has_state:
        norm_state = when(
            length(regexp_replace(col('STATE_FIPS'), r"[^0-9]", "")) > 0,
            lpad(regexp_replace(col('STATE_FIPS'), r"[^0-9]", ""), STATE_FIPS_WIDTH, "0")
        ).otherwise(col('STATE_FIPS'))
        change_mask = change_mask | (~norm_state.eqNullSafe(col('STATE_FIPS')))

    if has_county:
        county_digits_o = regexp_replace(col('COUNTY_FIPS'), r"[^0-9]", "")
        county_padded_o = lpad(county_digits_o, COUNTY_FIPS_WIDTH, "0")
        if has_state:
            county_padded_o = when(
                (length(regexp_replace(col('STATE_FIPS'), r"[^0-9]", "")) > 0)
                & (length(county_digits_o) > 0),
                concat(
                    lpad(regexp_replace(col('STATE_FIPS'), r"[^0-9]", ""), STATE_FIPS_WIDTH, "0"),
                    substring(county_padded_o, 3, COUNTY_FIPS_WIDTH - STATE_FIPS_WIDTH)
                )
            ).otherwise(county_padded_o)
        norm_county = when(length(county_digits_o) > 0, county_padded_o).otherwise(col('COUNTY_FIPS'))
        change_mask = change_mask | (~norm_county.eqNullSafe(col('COUNTY_FIPS')))

    if has_ruca:
        norm_ruca_int = regexp_extract(col('RUCA_ZIP'), r"^\s*([0-9]+)", 1)
        norm_ruca = when(length(norm_ruca_int) == 0, col('RUCA_ZIP')) \
            .when(length(norm_ruca_int) <= RUCA_ZIP_MAX_WIDTH, norm_ruca_int) \
            .otherwise(lit(""))
        change_mask = change_mask | (~norm_ruca.eqNullSafe(col('RUCA_ZIP')))

    changed_rows: DataFrame = original.filter(change_mask)

    cf.write_pyspark_output_file(
        payspark_df=changed_rows,
        output_file_name=f"{examined_table_name.replace('.csv', '')}_fips_ruca_normalized.csv",
        output_data_folder_path=output_data_folder_path + "/fixers_output/"
                                + examined_table_name_parsed + "_fixer/" + this_fix_name + "/"
    )

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
