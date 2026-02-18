

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime, date
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse
import sys

cf =CommonFuncitons('NotUsed')


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
db_server                 = args.src1
db_name                   = args.src2
input_data_folder_path    = args.input_path
output_data_folder_path   = args.output_path
fixed_table_name          = args.fixed_table_name
spark = cf.get_spark_session("patid_orphans_fix")

this_fix_name = 'append_to_existing_lds_address_hx_fix_1.0'
examined_table_name_parsed = examined_table_name.replace('mapped_','').replace('.csv','')

fixed_lds_address_history_table_name = 'fixed_lds_address_history.csv'

###################################################################################################################################
###################################################################################################################################
def to_date(val):
    fmt = "%Y-%m-%d"

    if isinstance(val, str):
        return datetime.strptime(val, fmt).date()
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    return None


###################################################################################################################################
###################################################################################################################################
def get_period_end_date(existing_period_start_date, fixed_period_start_date):


    existing_dt = to_date(existing_period_start_date)

    fixed_dt = to_date(fixed_period_start_date)

    try:

        if existing_dt < fixed_dt:

            return fixed_period_start_date
        else:
            return existing_period_start_date
    except:

            return existing_period_start_date

get_period_end_date_udf = udf(get_period_end_date, StringType())
        
                
###################################################################################################################################
###################################################################################################################################


def get_period_start_date(existing_period_start_date, fixed_period_start_date):


    existing_dt = to_date(existing_period_start_date)

    fixed_dt = to_date(fixed_period_start_date)
    
    try:

        if existing_dt < fixed_dt:

            return existing_period_start_date
        else:
            return fixed_period_start_date
    
    except:

        return existing_period_start_date

        
get_period_start_date_udf =  udf(get_period_start_date, StringType())



try:

    cf.print_fixer_status(

        current_count = '**',
        total_count   ='**',
        fix_type      = 'common', 
        fix_name      = this_fix_name
        
        )


    fixed_lds_address_history         = spark.read.load(output_data_folder_path+fixed_lds_address_history_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')


    existing_lds_address_history =   cf.read_from_snowflake(
        
        db_server    = db_server,
        db_name      = db_name,
        schema_name  = 'PUBLIC_18R2' ,
        table_name   = 'LDS_ADDRESS_HISTORY'
            )


    existing_lds_address_history = existing_lds_address_history.select(

                                    existing_lds_address_history['ADDRESSID'].alias("ADDRESSID"),
                                    existing_lds_address_history['PATID'].alias("PATID"),
                                    existing_lds_address_history['ADDRESS_USE'].alias("ADDRESS_USE"),
                                    existing_lds_address_history['ADDRESS_TYPE'].alias("ADDRESS_TYPE"),
                                    existing_lds_address_history['ADDRESS_PREFERRED'].alias("ADDRESS_PREFERRED"),
                                    existing_lds_address_history['ADDRESS_CITY'].alias("ADDRESS_CITY"),
                                    existing_lds_address_history['ADDRESS_STATE'].alias("ADDRESS_STATE"),
                                    existing_lds_address_history['ADDRESS_ZIP5'].alias("ADDRESS_ZIP5"),
                                    existing_lds_address_history['ADDRESS_ZIP9'].alias("ADDRESS_ZIP9"),
                                    existing_lds_address_history['ADDRESS_COUNTY'].alias("ADDRESS_COUNTY"),
                                    existing_lds_address_history['ADDRESS_PERIOD_START'].alias("ADDRESS_PERIOD_START"),
                                    existing_lds_address_history['ADDRESS_PERIOD_END'].alias("ADDRESS_PERIOD_END"),
                                    existing_lds_address_history['STATE_FIPS'].alias("STATE_FIPS"),
                                    existing_lds_address_history['COUNTY_FIPS'].alias("COUNTY_FIPS"),
                                    existing_lds_address_history['RUCA_ZIP'].alias("RUCA_ZIP"),
                                    existing_lds_address_history['CURRENT_ADDRESS_FLAG'].alias("CURRENT_ADDRESS_FLAG"),
                                    existing_lds_address_history['UPDATED'].alias("UPDATED"),
                                    existing_lds_address_history['SOURCE'].alias("SOURCE"),
    )



    original_order = existing_lds_address_history.columns

    rows_in_existing_lds_not_in_fixed_lds =  existing_lds_address_history.join(fixed_lds_address_history.select("PATID").distinct(), on="PATID", how="left_anti") 

    rows_in_existing_lds_not_in_fixed_lds = rows_in_existing_lds_not_in_fixed_lds.select(*original_order)




    slim_fixed_lds_address_history = fixed_lds_address_history.select (

        fixed_lds_address_history['PATID'],
        fixed_lds_address_history['ADDRESS_ZIP5'],
        fixed_lds_address_history['ADDRESS_PERIOD_START']   ).withColumnRenamed('PATID', 'LEFT_PATID')\
                                                                .withColumnRenamed('ADDRESS_ZIP5', 'LEFT_ADDRESS_ZIP5')\
                                                                .withColumnRenamed('ADDRESS_PERIOD_START', 'LEFT_ADDRESS_PERIOD_START')



    joined_ld_different_zip5 =   existing_lds_address_history.join(slim_fixed_lds_address_history,slim_fixed_lds_address_history["LEFT_PATID"]== existing_lds_address_history['PATID'], how = 'inner'  )\
                                                .filter( (existing_lds_address_history["ADDRESS_ZIP5"] != slim_fixed_lds_address_history["LEFT_ADDRESS_ZIP5"])| (existing_lds_address_history["ADDRESS_ZIP5"].isNull()) )  


    shared_lds_modified_zip_different = joined_ld_different_zip5.select(

                                    joined_ld_different_zip5['ADDRESSID'].alias("ADDRESSID"),
                                    joined_ld_different_zip5['PATID'].alias("PATID"),
                                    joined_ld_different_zip5['ADDRESS_USE'].alias("ADDRESS_USE"),
                                    joined_ld_different_zip5['ADDRESS_TYPE'].alias("ADDRESS_TYPE"),
                                    joined_ld_different_zip5['ADDRESS_PREFERRED'].alias("ADDRESS_PREFERRED"),
                                    joined_ld_different_zip5['ADDRESS_CITY'].alias("ADDRESS_CITY"),
                                    joined_ld_different_zip5['ADDRESS_STATE'].alias("ADDRESS_STATE"),
                                    joined_ld_different_zip5['ADDRESS_ZIP5'].alias("ADDRESS_ZIP5"),
                                    joined_ld_different_zip5['ADDRESS_ZIP9'].alias("ADDRESS_ZIP9"),
                                    joined_ld_different_zip5['ADDRESS_COUNTY'].alias("ADDRESS_COUNTY"),
                                    joined_ld_different_zip5['ADDRESS_PERIOD_START'].alias("ADDRESS_PERIOD_START"),
                                    get_period_end_date_udf(joined_ld_different_zip5['ADDRESS_PERIOD_START'],joined_ld_different_zip5['LEFT_ADDRESS_PERIOD_START']).alias("ADDRESS_PERIOD_END"),
                                    joined_ld_different_zip5['STATE_FIPS'].alias("STATE_FIPS"),
                                    joined_ld_different_zip5['COUNTY_FIPS'].alias("COUNTY_FIPS"),
                                    joined_ld_different_zip5['RUCA_ZIP'].alias("RUCA_ZIP"),
                                    joined_ld_different_zip5['CURRENT_ADDRESS_FLAG'].alias("CURRENT_ADDRESS_FLAG"),
                                    joined_ld_different_zip5['UPDATED'].alias("UPDATED"),
                                    joined_ld_different_zip5['SOURCE'].alias("SOURCE"),


    )


    joined_lds_same_zip5 =   existing_lds_address_history.join(slim_fixed_lds_address_history,slim_fixed_lds_address_history["LEFT_PATID"]== existing_lds_address_history['PATID'], how = 'inner'  )\
                                                .filter( existing_lds_address_history["ADDRESS_ZIP5"] == slim_fixed_lds_address_history["LEFT_ADDRESS_ZIP5"] )  


    shared_lds_modified_zip_same = joined_lds_same_zip5.select(

                                    joined_lds_same_zip5['ADDRESSID'].alias("ADDRESSID"),
                                    joined_lds_same_zip5['PATID'].alias("PATID"),
                                    joined_lds_same_zip5['ADDRESS_USE'].alias("ADDRESS_USE"),
                                    joined_lds_same_zip5['ADDRESS_TYPE'].alias("ADDRESS_TYPE"),
                                    joined_lds_same_zip5['ADDRESS_PREFERRED'].alias("ADDRESS_PREFERRED"),
                                    joined_lds_same_zip5['ADDRESS_CITY'].alias("ADDRESS_CITY"),
                                    joined_lds_same_zip5['ADDRESS_STATE'].alias("ADDRESS_STATE"),
                                    joined_lds_same_zip5['ADDRESS_ZIP5'].alias("ADDRESS_ZIP5"),
                                    joined_lds_same_zip5['ADDRESS_ZIP9'].alias("ADDRESS_ZIP9"),
                                    joined_lds_same_zip5['ADDRESS_COUNTY'].alias("ADDRESS_COUNTY"),
                                    get_period_start_date_udf(joined_lds_same_zip5['ADDRESS_PERIOD_START'],joined_lds_same_zip5['LEFT_ADDRESS_PERIOD_START']).alias("ADDRESS_PERIOD_START"),
                                    get_period_end_date_udf(joined_lds_same_zip5['ADDRESS_PERIOD_START'],joined_lds_same_zip5['LEFT_ADDRESS_PERIOD_START']).alias("ADDRESS_PERIOD_END"),
                                    joined_lds_same_zip5['STATE_FIPS'].alias("STATE_FIPS"),
                                    joined_lds_same_zip5['COUNTY_FIPS'].alias("COUNTY_FIPS"),
                                    joined_lds_same_zip5['RUCA_ZIP'].alias("RUCA_ZIP"),
                                    joined_lds_same_zip5['CURRENT_ADDRESS_FLAG'].alias("CURRENT_ADDRESS_FLAG"),
                                    joined_lds_same_zip5['UPDATED'].alias("UPDATED"),
                                    joined_lds_same_zip5['SOURCE'].alias("SOURCE"),


    )


    rows_in_fixed_lds_not_in_existing_lds =  fixed_lds_address_history.join(existing_lds_address_history.select("PATID").distinct(), on="PATID", how="left_anti") 


    rows_in_fixed_lds_not_in_existing_lds = rows_in_fixed_lds_not_in_existing_lds.select(*original_order)


                                                                                                            
    final_lds_address_history = rows_in_existing_lds_not_in_fixed_lds.union(shared_lds_modified_zip_different)\
                                                                        .union(shared_lds_modified_zip_same)\
                                                                        .union(rows_in_fixed_lds_not_in_existing_lds)
                                                            



    cf.write_pyspark_output_file(
                            payspark_df = final_lds_address_history,
                            output_file_name = fixed_lds_address_history_table_name,
                            output_data_folder_path= output_data_folder_path)







    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = input_partner_name.lower(),
                            job     = this_fix_name,
                            text    = str(e))

