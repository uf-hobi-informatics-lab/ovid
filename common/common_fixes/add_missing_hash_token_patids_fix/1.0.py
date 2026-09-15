

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import col, lit, when, current_timestamp
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
input_data_folder         = args.data_folder
examined_table_name       = args.table
input_partner_name        = args.partner
patid_source_table        = args.src1
input_data_folder_path    = args.input_path
output_data_folder_path   = args.output_path
fixed_table_name          = args.fixed_table_name
spark = cf.get_spark_session("add_missing_hash_token_patids_fix")

this_fix_name = 'common_add_missing_hash_token_patids_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_', '').replace('.csv', '')

TOKEN_COLUMNS = [
    'TOKEN_01', 'TOKEN_02', 'TOKEN_03', 'TOKEN_04', 'TOKEN_05',
    'TOKEN_06', 'TOKEN_07', 'TOKEN_08', 'TOKEN_09', 'TOKEN_12',
    'TOKEN_14', 'TOKEN_15', 'TOKEN_16', 'TOKEN_17', 'TOKEN_18',
    'TOKEN_23', 'TOKEN_24', 'TOKEN_25', 'TOKEN_26', 'TOKEN_29',
    'TOKEN_30', 'TOKEN_101', 'TOKEN_102', 'TOKEN_103', 'TOKEN_104',
    'TOKEN_105', 'TOKEN_106', 'TOKEN_107', 'TOKEN_108', 'TOKEN_109',
    'TOKEN_110', 'TOKEN_111',
]


try:

    cf.print_fixer_status(
        current_count='**',
        total_count='**',
        fix_type='common',
        fix_name=this_fix_name
    )

    # Load demographic table (source of all valid PATIDs) and hash_token table
    demographic_table = cf.spark_read(input_data_folder_path + patid_source_table, spark)
    hash_token_table = cf.spark_read(output_data_folder_path + fixed_table_name, spark)

    # Find PATIDs in demographic that are NOT in hash_token
    demographic_patids = demographic_table.select(col('PATID').alias('DEMO_PATID')).distinct()
    hash_token_patids = hash_token_table.select(col('PATID').alias('HT_PATID')).distinct()

    missing_patids = demographic_patids.join(
        hash_token_patids,
        demographic_patids['DEMO_PATID'] == hash_token_patids['HT_PATID'],
        how='left_anti'
    ).select(col('DEMO_PATID').alias('PATID'))

    missing_count = missing_patids.count()

    if missing_count > 0:

        # Determine the most common TOKEN_ENCRYPTION_KEY from existing hash_token rows
        most_common_key_row = (
            hash_token_table
            .filter(col('TOKEN_ENCRYPTION_KEY').isNotNull())
            .groupBy('TOKEN_ENCRYPTION_KEY')
            .count()
            .orderBy(col('count').desc())
            .first()
        )

        if most_common_key_row is not None:
            encryption_key_value = most_common_key_row['TOKEN_ENCRYPTION_KEY']
        else:
            encryption_key_value = ''

        # Build stub rows: PATID + null tokens + encryption key + UPDATED + SOURCE
        stub_rows = missing_patids.select(col('PATID'))

        for token_col in TOKEN_COLUMNS:
            stub_rows = stub_rows.withColumn(token_col, lit(None).cast(StringType()))

        stub_rows = stub_rows.withColumn('TOKEN_ENCRYPTION_KEY', lit(encryption_key_value))
        stub_rows = stub_rows.withColumn('UPDATED', lit(datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
        stub_rows = stub_rows.withColumn('SOURCE', lit(input_partner_name.upper()))

        # Ensure column order matches the existing hash_token table
        stub_rows = stub_rows.select(hash_token_table.columns)

        # Union stub rows with existing hash_token
        combined_table = hash_token_table.unionByName(stub_rows)

        cf.write_pyspark_output_file(
            payspark_df=combined_table,
            output_file_name=fixed_table_name,
            output_data_folder_path=output_data_folder_path
        )

        # Write diagnostics: list of missing PATIDs that were backfilled
        cf.write_pyspark_output_file(
            payspark_df=missing_patids,
            output_file_name=f"{examined_table_name.replace('.csv', '')}_backfilled_missing_patids.csv",
            output_data_folder_path=output_data_folder_path + "/fixers_output/" + examined_table_name_parsed + "_fixer/" + this_fix_name + "/"
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
