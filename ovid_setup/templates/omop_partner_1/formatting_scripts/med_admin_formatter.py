###################################################################################################################################

# This script will convert an OMOP drug_exposure table to a PCORnet format as the med_admin table

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

# Loading the drug_exposure table to be converted to the med_admin table

###################################################################################################################################

    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'

    drug_exposure_table_name   = 'Drug_Exposure.txt'

    drug_exposure = spark.read.load(input_data_folder_path+drug_exposure_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    filter_values = ["Inpatient Administration","Inpatient administration","Inpatient","Medication list entry","Physician administered drug (identified from EHR order)"]
    filtered_drug_exposure = drug_exposure.filter(col("drug_type").isin(filter_values))


 ###################################################################################################################################

    def get_time_from_datetime(val_time):
        # Parse the input string into a datetime object
        datetime_object = datetime.strptime(val_time, "%Y-%m-%d %H:%M:%S")

        # Format the datetime object as a string in "yyyy-mm-dd" format
        formatted_time = datetime_object.strftime("%H:%M")

        return formatted_time
    
    convert_and_format_time_udf = udf(get_time_from_datetime, StringType()) 



    ###################################################################################################################################

    #Converting the fileds to PCORNet med_admin Format

    ###################################################################################################################################

    med_admin = filtered_drug_exposure.select(      filtered_drug_exposure['drug_exposure_id'].alias("MEDADMINID"),
                                                    filtered_drug_exposure['person_id'].alias("PATID"),
                                                    filtered_drug_exposure['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    lit('').alias("PRESCRIBINGID"),
                                                    filtered_drug_exposure['provider_id'].alias("MEDADMIN_PROVIDERID"),
                                                    filtered_drug_exposure['drug_exposure_start_date'].alias("MEDADMIN_START_DATE"),
                                                    convert_and_format_time_udf(filtered_drug_exposure['drug_exposure_start_datetime']).alias("MEDADMIN_START_TIME"),
                                                    filtered_drug_exposure['drug_exposure_end_date'].alias("MEDADMIN_STOP_DATE"),
                                                    convert_and_format_time_udf(filtered_drug_exposure['drug_exposure_end_datetime']).alias("MEDADMIN_STOP_TIME"),
                                                    filtered_drug_exposure['drug_code_type'].alias("MEDADMIN_TYPE"),
                                                    filtered_drug_exposure['drug_code'].alias("MEDADMIN_CODE"),
                                                    filtered_drug_exposure['dose_ordered'].alias("MEDADMIN_DOSE_ADMIN"),
                                                    filtered_drug_exposure['dose_unit'].alias("MEDADMIN_DOSE_ADMIN_UNIT"),
                                                    filtered_drug_exposure['route'].alias("MEDADMIN_ROUTE"),
                                                    lit('OD').alias("MEDADMIN_SOURCE"),
                                                    filtered_drug_exposure['drug_code'].alias("RAW_MEDADMIN_MED_NAME"),
                                                    filtered_drug_exposure['drug_code'].alias("RAW_MEDADMIN_CODE"),
                                                    filtered_drug_exposure['dose_ordered_source_value'].alias("RAW_MEDADMIN_DOSE_ADMIN"),
                                                    filtered_drug_exposure['dose_unit'].alias("RAW_MEDADMIN_DOSE_ADMIN_UNIT"),
                                                    filtered_drug_exposure['route_source_value'].alias("RAW_MEDADMIN_ROUTE"),

                                                    
                                                    
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                      payspark_df = med_admin,
                      output_file_name = "formatted_med_admin.csv",
                      output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()


except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'med_admin_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')



