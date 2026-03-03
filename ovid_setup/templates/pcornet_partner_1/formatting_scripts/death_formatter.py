
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


try:
    ###################################################################################################################################

    # Loading the omop_death table to be converted to the pcornet_death table
    # loading the care_site, location, and visit_payer as they are been used to retrive some data for the mapping

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'


    death_table_name       = '*Death_2*'





    ###################################################################################################################################

    #Converting the fileds to PCORNet pcornet_death Format

    ###################################################################################################################################
    try:
        death_IN = spark.read.load(input_data_folder_path+death_table_name,format="csv", sep="~", inferSchema="false", header="true", quote= '"')
        death = death_IN.select(                 
            death_IN['patid'].alias('PATID'),
            cf.format_date_udf(death_IN['death_date']).alias('DEATH_DATE'),
            death_IN['death_date_impute'].alias('DEATH_DATE_IMPUTE'),
            death_IN['death_source'].alias('DEATH_SOURCE'),
            death_IN['death_match_confidence'].alias('DEATH_MATCH_CONFIDENCE')
        )
    except:
        death_IN = spark.read.load(input_data_folder_path+death_table_name,format="csv", sep=";", inferSchema="false", header="true", quote= '"')
        death = death_IN.select(                 
            death_IN['PATID'].alias('PATID'),
            cf.format_date_udf(death_IN['DEATH_DATE']).alias('DEATH_DATE'),
            death_IN['DEATH_DATE_IMPUTE'].alias('DEATH_DATE_IMPUTE'),
            death_IN['DEATH_SOURCE'].alias('DEATH_SOURCE'),
            death_IN['DEATH_MATCH_CONFIDENCE'].alias('DEATH_MATCH_CONFIDENCE')
        )



    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################


    cf.write_pyspark_output_file(
                        payspark_df = death,
                        output_file_name = "formatted_death.csv",
                        output_data_folder_path= formatter_output_data_folder_path)


    spark.stop()




except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'death_formatter.py' ,
                            text = str(e)
                            )

    # cf.print_with_style(str(e), 'danger red')







