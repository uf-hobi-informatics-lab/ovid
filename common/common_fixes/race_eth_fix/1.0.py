import pyspark
from pyspark.sql.functions import *
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
parser.add_argument("-sr1", "--src1")   # pass 'RACE'
parser.add_argument("-sr2", "--src2")   # pass 'HISPANIC'
args = parser.parse_args()

input_data_folder         = args.data_folder
examined_table_name       = args.table
input_partner_name        = args.partner
race_col                  = args.src1 or 'RACE'       # PCORnet RACE code column
hisp_col                  = args.src2 or 'HISPANIC'   # PCORnet HISPANIC code column
input_data_folder_path    = args.input_path
output_data_folder_path   = args.output_path
fixed_table_name          = args.fixed_table_name

spark = cf.get_spark_session("race_eth_fix")
this_fix_name = 'common_race_eth_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_','').replace('.csv','')

def blank(c):
    return col(c).isNull() | (trim(col(c)) == '')

try:
    cf.print_fixer_status(current_count='**', total_count='**', fix_type='common', fix_name=this_fix_name)

    df = spark.read.load(output_data_folder_path + fixed_table_name,format="csv", sep="\t", inferSchema=False, header=True, quote='"' )

    rc = upper(trim(col(race_col)))  # expects 01..06, OT, UN, R
    hc = upper(trim(col(hisp_col)))  # expects Y, N, UN, NI, OT, R
    
    fixed_df = (
        df
        .withColumn('RACE_ETH_AI_AN',    when(rc == '01', 'Y').otherwise(None))
        .withColumn('RACE_ETH_ASIAN',    when(rc == '02', 'Y').otherwise(None))
        .withColumn('RACE_ETH_BLACK',    when(rc == '03', 'Y').otherwise(None))
        .withColumn('RACE_ETH_NH_PI',    when(rc == '04', 'Y').otherwise(None))
        .withColumn('RACE_ETH_WHITE',    when(rc == '05', 'Y').otherwise(None))
        .withColumn('RACE_ETH_HISPANIC', when(hc == 'Y',  'Y').otherwise(None))
        .withColumn(
        'RACE_ETH_MISSING',
            when(
                    (hc == 'Y') & rc.isin('R','07','7','UN','NI','OT'), None  # Hispanic=Y + race in {R, UN, NI, OT} => null
                ).when(
                    rc.isin('R','07','7'), lit('R')                                # Refused race
                ).when(
                    (rc == 'OT') & (hc == 'N'), lit('OT')                          # Other + Non-Hispanic
                ).when(
                    rc == 'UN', lit('UN')                                          # Unknown race
                ).when(
                    rc.isNull() & hc.isNull(), lit('NI')                           # No information
                ).otherwise(None)
            )
    )

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
        partner=input_partner_name.lower() if input_partner_name else 'unknown',
        job=this_fix_name,
        text=str(e)
    )