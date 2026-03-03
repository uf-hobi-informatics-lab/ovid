###################################################################################################################################

# This script will convert an OMOP drug_exposure table to a PCORnet format as the immunization table

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


try: 


    ###################################################################################################################################

    # Loading the drug_exposure table to be converted to the immunization table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'



    drug_exposure_table_name       = 'drug_exposure.csv'
    drug_exposure_sup_table_name   = 'drug_exposure_sup.csv'


    drug_exposure = spark.read.load(input_data_folder_path+drug_exposure_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
    drug_exposure_sup = spark.read.load(input_data_folder_path+drug_exposure_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("drug_exposure_id",      "drug_exposure_id_sup")

    concept       = cf.spark_read(concept_table_path,spark)
    immunization_concept = concept.filter(concept.vocabulary_id == 'CVX').withColumnRenamed("concept_name", "drug_concept_name").withColumnRenamed("vocabulary_id", "drug_vocabulary_id").withColumnRenamed("concept_code", "drug_concept_code")
    route_concept = concept.filter(concept.domain_id == 'Route').withColumnRenamed("concept_name", "route_concept_name")




    ###################################################################################################################################

    # This function will attemp to return the float value of an input

    ###################################################################################################################################

    def get_float_format( val):
        float_val = None

        try:
            # remove ','
            val = str(val).replace(',', '')
            float_val = float(val)
        except Exception:
            # cls.logger.warning("Unable to format_float({}). Will use None".format(val))  # noqa
            pass
        return float_val

    get_float_format_udf = udf(get_float_format, StringType())


    ###################################################################################################################################

    #Converting the fields to PCORNet immunization Format

    ###################################################################################################################################



    joined_drug_exposure = drug_exposure.join(immunization_concept, immunization_concept['concept_id']==drug_exposure['drug_concept_id'], how='inner').drop("concept_id")\
                                            .join(drug_exposure_sup, drug_exposure_sup['drug_exposure_id_sup']== drug_exposure['drug_exposure_id'], how = 'left')\
                                            .join(route_concept, route_concept['concept_id']== drug_exposure['route_concept_id'], how = 'left')

    immunization = joined_drug_exposure.select(   joined_drug_exposure['drug_exposure_id'].alias("IMMUNIZATIONID"),
                                                    joined_drug_exposure['person_id'].alias("PATID"),
                                                    joined_drug_exposure['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    joined_drug_exposure['procedure_occurrence_id'].alias("PROCEDURESID"),
                                                    joined_drug_exposure['provider_id'].alias("VX_PROVIDERID"),
                                                    cf.format_date_udf(joined_drug_exposure['drug_exposure_record_date']).alias("VX_RECORD_DATE"),
                                                    cf.format_date_udf(joined_drug_exposure['drug_exposure_start_date']).alias("VX_ADMIN_DATE"),
                                                    joined_drug_exposure['drug_vocabulary_id'].alias("VX_CODE_TYPE"),
                                                    joined_drug_exposure['drug_concept_code'].alias("VX_CODE"),
                                                    joined_drug_exposure['vx_status'].alias("VX_STATUS"),
                                                    joined_drug_exposure['vx_status_reason'].alias("VX_STATUS_REASON"),
                                                    lit('DR').alias("VX_SOURCE"),
                                                    joined_drug_exposure['days_supply'].alias("VX_DOSE"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("VX_DOSE_UNIT"),
                                                    joined_drug_exposure['route_concept_id'].alias("VX_ROUTE"),
                                                    joined_drug_exposure['body_site_source_value'].alias("VX_BODY_SITE"),
                                                    joined_drug_exposure['vx_manufatcurer'].alias("VX_MANUFACTURER"),
                                                    joined_drug_exposure['lot_number'].alias("VX_LOT_NUM"),
                                                    cf.format_date_udf(joined_drug_exposure['drug_exposure_end_date']).alias("VX_EXP_DATE"),
                                                    joined_drug_exposure['drug_concept_name'].alias("RAW_VX_NAME"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RAW_VX_CODE"),
                                                    joined_drug_exposure['drug_vocabulary_id'].alias("RAW_VX_CODE_TYPE"),
                                                    joined_drug_exposure['days_supply'].alias("RAW_VX_DOSE"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("RAW_VX_DOSE_UNIT"),
                                                    joined_drug_exposure['route_source_value'].alias("RAW_VX_ROUTE"),
                                                    joined_drug_exposure['body_site_source_value'].alias("RAW_VX_BODY_SITE"),
                                                    joined_drug_exposure['vx_status'].alias("RAW_VX_STATUS"),
                                                    joined_drug_exposure['vx_status_reason'].alias("RAW_VX_STATUS_REASON"),
                                                    joined_drug_exposure['vx_manufatcurer'].alias("RAW_VX_MANUFACTURER"),

                                                    
                                                    
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = immunization,
                        output_file_name = "formatted_immunization.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'immunization_formatter.py' ,
                            text    = str(e))





