###################################################################################################################################

# This script will convert an OMOP lds_address_history table to a PCORnet format as the lds_address_history table

###################################################################################################################################
import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime
from pyspark.sql.functions import *
from commonFunctions import CommonFuncitons
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-p", "--partner_name")

args = parser.parse_args()
input_data_folder = args.data_folder
partner_name = args.partner_name
cf =CommonFuncitons(partner_name)
# Create SparkSession
spark = cf.get_spark_session("ovid")


try:
###################################################################################################################################

    # Loading the lds_address_history table to be converted to the lds_address_history table

###################################################################################################################################
    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    lds_address_history_table_name         = 'address.csv'
    location_table_name                    = 'location.csv'


    lds_address_history = spark.read.load(input_data_folder_path+lds_address_history_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    location = spark.read.load(input_data_folder_path+location_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').select("location_id", "city", "state", "zip")
    location = location.select(location['location_id'].alias("location_id_right"), location['city'], location['state'], location['zip'] )


    joined_lds_address_history = lds_address_history.join(location, location['location_id_right']==lds_address_history['location_id'], how='left')

    # Optional source columns: OMOP address extracts vary by site, so fall back to '' when absent.
    # CURRENT_ADDRESS_FLAG is mapped through lds_address_history_current_address_flag_dict.
    # STATE_FIPS / COUNTY_FIPS / RUCA_ZIP are derived by the mapper from ZIP5/ZIP9 using
    # common/lists/FULL_GEOCODING_*.csv(.gz) and RUCA lists, so they are not formatted here.
    def optional_col(name):
        return col(name) if name in joined_lds_address_history.columns else lit('')

    # OMOP location.zip may hold a 5-digit ZIP or a ZIP+4 ("12345-6789" / "123456789").
    # The mapper joins ADDRESS_ZIP9 / ADDRESS_ZIP5 to the geocoding reference, so normalize both.
    zip_digits = regexp_replace(coalesce(col('zip'), lit('')), r'[^0-9]', '')
    zip9_digits = regexp_replace(coalesce(optional_col('zip9'), lit('')), r'[^0-9]', '')
    address_zip5 = when(length(zip_digits) >= 5, substring(zip_digits, 1, 5)).otherwise(lit(''))
    address_zip9 = (when(length(zip9_digits) == 9, zip9_digits)
                    .when(length(zip_digits) == 9, zip_digits)
                    .otherwise(lit('')))

    ###################################################################################################################################

    #Converting the fileds to PCORNet lds_address_history Format

    ###################################################################################################################################

    lds_address_history = joined_lds_address_history.select(            
                                                    joined_lds_address_history['address_id'].alias("ADDRESSID"),
                                                    joined_lds_address_history['person_id'].alias("PATID"),
                                                    joined_lds_address_history['address_use'].alias("ADDRESS_USE"),
                                                    joined_lds_address_history['address_type'].alias("ADDRESS_TYPE"),
                                                    joined_lds_address_history['address_preferred'].alias("ADDRESS_PREFERRED"),
                                                    joined_lds_address_history['city'].alias("ADDRESS_CITY"), 
                                                    joined_lds_address_history['state'].alias("ADDRESS_STATE"),
                                                    address_zip5.alias("ADDRESS_ZIP5"),
                                                    address_zip9.alias("ADDRESS_ZIP9"),
                                                    optional_col('county').alias("ADDRESS_COUNTY"),
                                                    joined_lds_address_history['address_period_start'].alias("ADDRESS_PERIOD_START"),
                                                    joined_lds_address_history['address_period_end'].alias("ADDRESS_PERIOD_END"),
                                                    optional_col('current_address_flag').alias("CURRENT_ADDRESS_FLAG"),

                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                      payspark_df = lds_address_history,
                      output_file_name = "formatted_lds_address_history.csv",
                      output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'lds_address_history_formatter.py' ,
                            text = str(e)
                            )
