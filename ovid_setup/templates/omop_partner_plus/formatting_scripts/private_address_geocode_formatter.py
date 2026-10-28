###################################################################################################################################

# This script will convert address data to a PCORnet format as the private_address_geocode table

###################################################################################################################################


import pyspark
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType
from datetime import datetime,date
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from commonFunctions import CommonFuncitons
import argparse
import re
from dateutil.relativedelta import relativedelta
from itertools import chain
import sys


parser = argparse.ArgumentParser()
parser.add_argument("-f", "--data_folder")
parser.add_argument("-p", "--partner_name")

args = parser.parse_args()
input_data_folder = args.data_folder
partner_name = args.partner_name
cf =CommonFuncitons(partner_name)
spark = cf.get_spark_session("ovid")

try:

  input_data_folder_path = f'/data/{input_data_folder}/'
  formatter_output_data_folder_path = f'/app/partners/{partner_name.lower()}/data/{input_data_folder}/formatter_output/'

  ###################################################################################################################################

  # Loading the lds_address_history and location tables

  ###################################################################################################################################

  lds_address_history_table_name         = 'address.csv'
  location_table_name                    = 'location.csv'

  lds_address_history = spark.read.load(input_data_folder_path+lds_address_history_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"')
  location = spark.read.load(input_data_folder_path+location_table_name,format="csv", sep="\t", inferSchema="false", header="true", quote= '"').select("location_id", "zip")
  location = location.select(location['location_id'].alias("location_id_right"), location['zip'] )

  joined_lds_address_history = lds_address_history.join(location, location['location_id_right']==lds_address_history['location_id'], how='left')

  address = joined_lds_address_history.select(
      col("address_id").alias("ADDRESSID"),
      cf.get_valid_zip5_udf(col("zip")).alias("ADDRESS_ZIP5"),
      lit("").alias("ADDRESS_ZIP9")
  )

  ###################################################################################################################################

  # Geocoding enrichment

  ###################################################################################################################################

  geocoding_table_path = cf.get_geocoding_path()  # FULL_GEOCODING_*.csv or .csv.gz
  geocoding = spark.read.load(geocoding_table_path,format="csv", sep=",", inferSchema="false", header="true", quote= '"')
  geocoding = geocoding.withColumn(
    "FULL_GEOCODE_COUNTY",
    concat(col("GEOCODE_STATE"), col("GEOCODE_COUNTY"))
    )

  geocoding_zip5 = (
    geocoding
    .withColumn("ZIP9", col("ZIP9").cast("string"))
    .filter(col("ZIP9").isNull() | (trim(col("ZIP9")) == ""))
    .select(
        col("ZIP5").cast("string").alias("ZIP5"),
        col("RUCA_2020").alias("RUCA_ZIP5"),
        col("GEOCODE_STATE").alias("STATE_ZIP5"),
        col("FULL_GEOCODE_COUNTY").alias("COUNTY_ZIP5"),
        col("ZCTA").alias("ZCTA_ZIP5")
    )
  )

  joined_geocoding = (
    address
    .join(
        geocoding,
        geocoding["ZIP9"] == address["ADDRESS_ZIP9"],
        how="left"
    )
    .join(
        geocoding_zip5,
        geocoding_zip5["ZIP5"] == address["ADDRESS_ZIP5"],
        how="left"
    )
  )

  mapping_fips_to_state_abbrev_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_state_abbrev_from_fips().items())])
  mapping_fips_to_countyName_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_countyName_from_fips().items())])
  mapping_zip_to_state_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_state_from_zip().items())])
  mapping_zip_to_county_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_county_from_zip().items())])
  mapping_zip_to_zcta_dict = create_map([lit(x) for x in chain.from_iterable(cf.get_zcta_from_zip().items())])

  def get_geo_accuracy(RUCA_2020, RUCA_ZIP5):
    if RUCA_2020 is None and RUCA_ZIP5 is None:
        return "NI"
    elif RUCA_2020 is not None:
        return "Z9"
    else:
        return "Z5"
  get_geo_accuracy_udf = udf(get_geo_accuracy, StringType())

  def get_shapefile(RUCA_2020, RUCA_ZIP5):
    if RUCA_2020 is None and RUCA_ZIP5 is None:
        return 'NI'
    elif RUCA_2020 is not None:
        return 'RUCA2020_ZIP9'
    else:
        return 'RUCA2020_ZIP5'
  get_shapefile_udf = udf(get_shapefile, StringType())

  def get_zcta(ZCTA, ZCTA_ZIP5):
    if ZCTA is None and ZCTA_ZIP5 is None:
        return None
    elif ZCTA is not None:
        return ZCTA
    else:
        return ZCTA_ZIP5
  get_zcta_udf = udf(get_zcta, StringType())

  private_address_geocode = joined_geocoding.select(
                                            concat(date_format(current_timestamp(), "yyyyMMddHHmmssSSS"), monotonically_increasing_id()).alias("GEOCODEID"),
                                            col("ADDRESSID").alias("ADDRESSID"),
                                            coalesce(mapping_fips_to_state_abbrev_dict[col("GEOCODE_STATE")], mapping_zip_to_state_dict[col("ADDRESS_ZIP5")], lit("")).alias("GEOCODE_STATE"),
                                            coalesce(mapping_fips_to_countyName_dict[concat(col("GEOCODE_STATE"), col("GEOCODE_COUNTY"))], mapping_zip_to_county_dict[col("ADDRESS_ZIP5")], lit("")).alias("GEOCODE_COUNTY"),
                                            coalesce(col("X"), lit("")).alias("GEOCODE_LONGITUDE"),
                                            coalesce(col("Y"), lit("")).alias("GEOCODE_LATITUDE"),
                                            coalesce(col("GEOCODE_BLOCK_15dig"), lit("")).alias("GEOCODE_BLOCK"),
                                            coalesce(col("GEOCODE_TRACT_11dig"), lit("")).alias("GEOCODE_TRACT"),
                                            coalesce(col("GEOCODE_GROUP"), lit("")).alias("GEOCODE_GROUP"),
                                            get_zcta_udf(col("ZCTA"), col("ZCTA_ZIP5")).alias("GEOCODE_ZCTA"),
                                            lit("").alias("GEOCODE_CUSTOM"),
                                            lit("").alias("GEOCODE_CUSTOM_TEXT"),
                                            get_shapefile_udf(col("RUCA_2020"), col("RUCA_ZIP5")).alias("SHAPEFILE"),
                                            get_geo_accuracy_udf(col("RUCA_2020"), col("RUCA_ZIP5")).alias("GEO_ACCURACY"),
                                            lit("FULL_GEOCODING_202603/RUCA-2020").alias("GEO_PROV_REF"),
                                            date_format(current_timestamp(), "yyyyMMdd").alias("ASSIGNMENT_DATE")
                                        )

  cf.write_pyspark_output_file(
                      payspark_df = private_address_geocode,
                      output_file_name = "formatted_private_address_geocode.csv",
                      output_data_folder_path= formatter_output_data_folder_path)

  spark.stop()

except Exception as e:
    spark.stop()
    cf.print_failure_message(
                            folder  = input_data_folder,
                            partner = partner_name.lower(),
                            job     = 'private_address_geocode_formatter.py' ,
                            text = str(e)
                            )
