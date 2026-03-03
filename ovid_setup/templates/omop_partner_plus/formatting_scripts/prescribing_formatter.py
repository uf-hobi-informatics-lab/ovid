###################################################################################################################################

# This script will convert an OMOP drug_exposure table to a PCORnet format as the prescribing table

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

    # Loading the drug_exposure table to be converted to the prescribing table

    ###################################################################################################################################


    input_data_folder_path               = f'/data/{input_data_folder}/'
    formatter_output_data_folder_path    = f'/app/partners/{partner_name}/data/{input_data_folder}/formatter_output/'
    concept_table_path                   = f'/app/common/cdm/omop_5_3/CONCEPT.csv'


    drug_exposure_table_name   = 'drug_exposure.csv'
    drug_exposure_sup_table_name   = 'drug_exposure_sup.csv'

    drug_exposure = spark.read.load(input_data_folder_path+drug_exposure_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')

    drug_exposure_sup = spark.read.load(input_data_folder_path+drug_exposure_sup_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').withColumnRenamed("drug_exposure_id",      "drug_exposure_id_sup")

    concept       = cf.spark_read(concept_table_path,spark)
    drug_concept = concept.filter(concept.domain_id == 'Drug').withColumnRenamed("concept_name", "drug_concept_name").withColumnRenamed("vocabulary_id", "drug_vocabulary_id").withColumnRenamed("concept_code", "drug_concept_code")
    prescribing_drug_type_concept = concept.filter((col("domain_id") == "Type Concept") & (col("concept_name").rlike(r"(?i)Prescri") ))
    route_concept = concept.filter(concept.domain_id == 'Route').withColumnRenamed("concept_name", "route_concept_name")


    # filter_values = ["Prescription written","Prescription","Home-Med","Prescription dispensed in pharmacy"]
    # filtered_drug_exposure = drug_exposure.filter(col("drug_type").isin(filter_values))


    joined_drug_exposure = drug_exposure.join(prescribing_drug_type_concept, prescribing_drug_type_concept['concept_id']==drug_exposure['drug_type_concept_id'], how='inner').drop("concept_id")\
                                            .join(drug_concept, drug_concept['concept_id']==drug_exposure['drug_concept_id'], how='left').drop("concept_id")\
                                            .join(drug_exposure_sup, drug_exposure_sup['drug_exposure_id_sup']== drug_exposure['drug_exposure_id'], how = 'left')\
                                            .join(route_concept, route_concept['concept_id']== drug_exposure['route_concept_id'], how = 'left')




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

    #Converting the fields to PCORNet prescribing Format

    ###################################################################################################################################

    prescribing = joined_drug_exposure.select(    joined_drug_exposure['drug_exposure_id'].alias("PRESCRIBINGID"),
                                                    joined_drug_exposure['person_id'].alias("PATID"),
                                                    joined_drug_exposure['visit_occurrence_id'].alias("ENCOUNTERID"),
                                                    joined_drug_exposure['provider_id'].alias("RX_PROVIDERID"),
                                                    joined_drug_exposure['drug_exposure_start_date'].alias("RX_ORDER_DATE"),
                                                    cf.get_time_from_datetime_udf(joined_drug_exposure['drug_exposure_start_datetime']).alias("RX_ORDER_TIME"),
                                                    joined_drug_exposure['drug_exposure_start_date'].alias("RX_START_DATE"),
                                                    joined_drug_exposure['drug_exposure_end_date'].alias("RX_END_DATE"),
                                                    joined_drug_exposure['days_supply'].alias("RX_DOSE_ORDERED"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("RX_DOSE_ORDERED_UNIT"),
                                                    get_float_format_udf(joined_drug_exposure['quantity']).alias("RX_QUANTITY"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RX_DOSE_FORM"),
                                                    joined_drug_exposure['refills'].alias("RX_REFILLS"),
                                                    joined_drug_exposure['days_supply'].alias("RX_DAYS_SUPPLY"),
                                                    joined_drug_exposure['rx_frequency'].alias("RX_FREQUENCY"),
                                                    joined_drug_exposure['sig'].alias("RX_PRN_FLAG"),
                                                    joined_drug_exposure['route_concept_name'].alias("RX_ROUTE"),
                                                    joined_drug_exposure['drug_vocabulary_id'].alias("RX_BASIS"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RXNORM_CUI"),
                                                    lit('OD').alias("RX_SOURCE"),
                                                    lit('OD').alias("RX_DISPENSE_AS_WRITTEN"),
                                                    joined_drug_exposure['drug_concept_name'].alias("RAW_RX_MED_NAME"),
                                                    joined_drug_exposure['rx_frequency'].alias("RAW_RX_FREQUENCY"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RAW_RXNORM_CUI"),
                                                    joined_drug_exposure['quantity'].alias("RAW_RX_QUANTITY"),
                                                    joined_drug_exposure['drug_concept_code'].alias("RAW_RX_NDC"),
                                                    joined_drug_exposure['days_supply'].alias("RAW_RX_DOSE_ORDERED"),
                                                    joined_drug_exposure['dose_unit_source_value'].alias("RAW_RX_DOSE_ORDERED_UNIT"),
                                                    joined_drug_exposure['route_source_value'].alias("RAW_RX_ROUTE"),
                                                    joined_drug_exposure['refills'].alias("RAW_RX_REFILLS"),

                                                    
                                                    
                                                    
                                                        )

    ###################################################################################################################################

    # Create the output file

    ###################################################################################################################################

    cf.write_pyspark_output_file(
                        payspark_df = prescribing,
                        output_file_name = "formatted_prescribing.csv",
                        output_data_folder_path= formatter_output_data_folder_path)

    spark.stop()



except Exception as e:

    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = 'omop_partner_plus',
                            job     = 'prescribing_formatter.py' ,
                            text    = str(e))





