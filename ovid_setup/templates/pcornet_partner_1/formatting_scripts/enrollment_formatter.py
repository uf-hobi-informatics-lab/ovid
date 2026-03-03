###################################################################################################################################

# This script will convert an OMOP enrollment_in table to a PCORnet format as the enrollment table

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

def fix_pcornet_partner_1_date (date_str):

    try:

        input_date = datetime.strptime(date_str, "%m/%d/%Y")

        output_date_str = input_date.strftime("%Y-%m-%d")

        return output_date_str
    except:
        return None
        
fix_pcornet_partner_1_date_udf = udf(fix_pcornet_partner_1_date, StringType())




try:

    ###################################################################################################################################

    # Loading the enrollment_in table to be converted to the enrollment table
    # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

    ###################################################################################################################################
    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    enrollment_in_table_name       = '*Enrollment*'

    enrollment_in = spark.read.load(input_data_folder_path+enrollment_in_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')




    ###################################################################################################################################

    #Converting the fileds to PCORNet enrollment Format

    ###################################################################################################################################
    try:

        enrollment_in = spark.read.load(input_data_folder_path+enrollment_in_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        enrollment = enrollment_in.select(              
            
                                                        enrollment_in['patid'].alias("PATID"),
                                                        fix_pcornet_partner_1_date_udf(enrollment_in['enr_start_date']).alias("ENR_START_DATE"),
                                                        fix_pcornet_partner_1_date_udf(enrollment_in['enr_end_date']).alias("ENR_END_DATE"),
                                                        enrollment_in['chart'].alias("CHART"),
                                                        enrollment_in['enr_basis'].alias("ENR_BASIS"),
                                                        
                                                            )
    except:
        enrollment_in = spark.read.load(input_data_folder_path+enrollment_in_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        enrollment = enrollment_in.select(              
            
                                                        enrollment_in['PATID'].alias("PATID"),
                                                        fix_pcornet_partner_1_date_udf(enrollment_in['ENR_START_DATE']).alias("ENR_START_DATE"),
                                                        fix_pcornet_partner_1_date_udf(enrollment_in['ENR_END_DATE']).alias("ENR_END_DATE"),
                                                        enrollment_in['CHART'].alias("CHART"),
                                                        enrollment_in['ENR_BASIS'].alias("ENR_BASIS"),
                                                        
                                                            )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = enrollment,
                        output_file_name = "formatted_enrollment.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()





except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'enrollment_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')




