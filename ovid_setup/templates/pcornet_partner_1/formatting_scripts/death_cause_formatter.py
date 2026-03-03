###################################################################################################################################

# This script will format the death_cause dataset to a PCORnet formatted death_cause table.

###################################################################################################################################

import pyspark
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



def truncate_8_chars(val):
    return val[:8]

truncate_8_chars_udf = udf(truncate_8_chars, StringType())
try:
        
    ###################################################################################################################################

    # Loading the death dataset to be formatted to the pcornet_death_cause table.

    ###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'

    death_table_name       = '*Death_Cause*'


    ###################################################################################################################################

    # Converting the fields to the PCORnet format.

    ###################################################################################################################################
    try:
        unformatted_death_cause = spark.read.load(input_data_folder_path+death_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        pcornet_death_cause = unformatted_death_cause.select(    
                                                        
                                                        unformatted_death_cause['patid'].alias("PATID"), 
                                                        truncate_8_chars_udf(col('death_cause')).alias("DEATH_CAUSE"),
                                                        unformatted_death_cause['death_cause_code'].alias("DEATH_CAUSE_CODE"),
                                                        unformatted_death_cause['death_cause_type'].alias("DEATH_CAUSE_TYPE"),
                                                        lit('L').alias("DEATH_CAUSE_SOURCE"),
                                                        unformatted_death_cause['death_cause_confidence'].alias("DEATH_CAUSE_CONFIDENCE"),
                                                            )
    except:
        unformatted_death_cause = spark.read.load(input_data_folder_path+death_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        pcornet_death_cause = unformatted_death_cause.select(    
                                                        
                                                        unformatted_death_cause['PATID'].alias("PATID"), 
                                                        truncate_8_chars_udf(col('DEATH_CAUSE')).alias("DEATH_CAUSE"),
                                                        unformatted_death_cause['DEATH_CAUSE_CODE'].alias("DEATH_CAUSE_CODE"),
                                                        unformatted_death_cause['DEATH_CAUSE_TYPE'].alias("DEATH_CAUSE_TYPE"),
                                                        lit('L').alias("DEATH_CAUSE_SOURCE"),
                                                        unformatted_death_cause['DEATH_CAUSE_CONFIDENCE'].alias("DEATH_CAUSE_CONFIDENCE"),
                                                            )
    ###################################################################################################################################

    # Create the output file.

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = pcornet_death_cause,
                        output_file_name = "formatted_death_cause.csv",
                        output_data_folder_path = formatter_output_data_folder_path)

    # Once the script has finished, time to shutdown the resources used to run the PySpark Cluster.
    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'death_cause_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')






