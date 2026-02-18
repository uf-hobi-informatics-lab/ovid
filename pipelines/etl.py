
from commonFunctions import CommonFuncitons as cf
import importlib
import sys
import os
import subprocess
import pyspark
from pyspark.sql import SparkSession
import argparse
from datetime import datetime
import pytz
from settings import  VALID_ETL_JOBS
from ovid_logging import setup_logging, get_logger, log_job_start, log_job_end, log_table_processing

from PIL import Image

master_url = os.environ["SPARK_URL"].strip().replace('"', '')

# Extract timestamp from master URL (format: spark://ovid-master-YYYYMMDD-HHMMSS:7077)
master_name = master_url.replace('spark://', '').replace(':7077', '')
timestamp = master_name.replace('ovid-master-', '')

print('*********************************************************************')
print(master_url)
print('*********************************************************************')


spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()


# spark = SparkSession.builder.master(os.environ["SPARK_URL"]).appName("Ovid").getOrCreate()
spark.sparkContext.setLogLevel('OFF')
spark.stop()

print("PYTHONPATH:", sys.path)
                                    
 

job_num = 0


###################################################################################################################################
# parsing the input arguments to select the partner name
###################################################################################################################################


parser = argparse.ArgumentParser()

parser.add_argument("-j", "--job", nargs="+")
parser.add_argument("-p", "--partner")
parser.add_argument("-t", "--table", nargs="+")
parser.add_argument("-db", "--database")
parser.add_argument("-s", "--db_server")
parser.add_argument("-f", "--data_folder",nargs="+")
parser.add_argument("-dt", "--database_type")
parser.add_argument("-dc", "--dc_check" ,nargs="+")
parser.add_argument("-ut", "--upload_type")
parser.add_argument("-mdb", "--merge_database")
parser.add_argument("-ms", "--merge_schema")



args = parser.parse_args()

input_jobs = args.job
input_partner = args.partner.lower()
input_tables = args.table
input_db = args.database
input_db_server = args.db_server
input_data_folders = args.data_folder
input_db_type = args.database_type
input_dc_checks = args.dc_check
input_upload_type = args.upload_type
input_merge_db = args.merge_database
input_merge_schema = args.merge_schema

# Initialize logging with timestamp and partner
logger = setup_logging(timestamp=timestamp, partner=input_partner)
logger.info(f"ETL Pipeline started - Partner: {input_partner}, Jobs: {input_jobs}, Tables: {input_tables}")




# cf =CommonFuncitons(input_partner)

logger.info("=" * 60)
logger.info("Starting ETL job execution")
logger.info("=" * 60)

print("""

                            ▄█  ░█▀▀▀ ░█       ░█▀▀█ ░█ ░█  █▀▀█ ░█▄ ░█ ░█▀▀█ ░█▀▀▀ ▀█▀ ░█▄ ░█  █▀▀█ ▀▀█▀▀ ░█▀▀▀█ ░█▀▀█ 
                             █  ░█▀▀▀ ░█    ▀▀ ░█    ░█▀▀█ ░█▄▄█ ░█░█░█ ░█ ▄▄ ░█▀▀▀ ░█  ░█░█░█ ░█▄▄█  ░█   ░█  ░█ ░█▄▄▀ 
                            ▄█▄ ░█    ░█▄▄█    ░█▄▄█ ░█ ░█ ░█ ░█ ░█  ▀█ ░█▄▄█ ░█▄▄▄ ▄█▄ ░█  ▀█ ░█ ░█  ░█   ░█▄▄▄█ ░█ ░█

                                                                                                                                                            
                                                                              """
                                                   )

###################################################################################################################################
# This function will get all the created formatter python script and return a list of thier names
###################################################################################################################################
def get_partner_formatters_list(partner):

    folder_path = '/app/partners/'+partner.lower()+'/formatting_scripts/'
    suffix = 'formatter.py' 
    file_names = os.listdir(folder_path)

    partner_formatters_list = [file for file in file_names if file.endswith(suffix)]

    return partner_formatters_list


###################################################################################################################################
# This function will get all the created formatter python script and return a list of thier names
###################################################################################################################################
def get_dc_checks_list():

    folder_path = '/app/dc_checks_scripts/'
    prefix = 'dc_check' 
    file_names = os.listdir(folder_path)

    dc_checks_list = [file for file in file_names if file.startswith(prefix)]

    return dc_checks_list

###################################################################################################################################
# This function will get all the created formatter python script and return a list of thier names
###################################################################################################################################
def get_partner_fixers_list(partner):

    folder_path = '/app/partners/'+partner.lower()+'/fixer_scripts/'
    suffix = 'fixer.py' 
    file_names = os.listdir(folder_path)

    partner_fixers_list = [file for file in file_names if file.endswith(suffix)]

    return partner_fixers_list

###################################################################################################################################
# This function will get all the created mappers python script and return a list of thier names
###################################################################################################################################

def get_partner_mappers_list(partner):
        


        partner_settings_path = "partners."+partner+".partner_settings"
        partner_settings = importlib.import_module(partner_settings_path)


        folder_path = f'/app/mapping_scripts/{partner_settings.mappers_type}/'

        suffix = 'mapper.py' 
        file_names = os.listdir(folder_path)

        partner_mappers_list = [file for file in file_names if file.endswith(suffix)]

        return partner_mappers_list

###################################################################################################################################
# This function will get all the list names of all the mapped files
###################################################################################################################################

def get_partner_uploads_list(folder_path):

       
        prefix = 'fixed_' 
        file_names = os.listdir(folder_path)

        fixed_tables_list = [file for file in file_names if file.startswith(prefix)]

        return fixed_tables_list



###################################################################################################################################
# This function will get all the list names of all the formatted files
###################################################################################################################################

def get_partner_formatted_files_list(folder_path):

       
        prefix = 'formatted_' 
        file_names = os.listdir(folder_path)

        formatted_tables_list = [file for file in file_names if file.startswith(prefix)]

        return formatted_tables_list


###################################################################################################################################
# This function will run the formatter script for a single table
###################################################################################################################################

def run_formatter(partner, formatter, folder):

    global job_num

    job_num = job_num +1
    cf.print_run_status(job_num,total_jobs_count,formatter, folder, partner)

    # command = ["python", "/app/partners/"+partner+"/formatting_scripts/"+formatter, '-f', folder , '' ]

    command = ["python", "/app/partners/"+partner+"/formatting_scripts/"+formatter, '-f', folder , '-p', partner  ]
    # Execute the command
    subprocess.run(" ".join(command), shell=True)



###################################################################################################################################
# This function will run the formatter script for a single table
###################################################################################################################################

def run_fixer(partner, fixer, folder):

    global job_num

    job_num = job_num +1
    cf.print_run_status(job_num,total_jobs_count,fixer, folder, partner)


    command = ["python", "/app/partners/"+partner+"/fixer_scripts/"+fixer, '-f', folder  ]
    # Execute the command
    subprocess.run(" ".join(command), shell=True)

###################################################################################################################################
# This function will run the deduplicator script for a single table
###################################################################################################################################

def run_deduplicator(partner, input_folder, output_folder):

    global job_num

    # process duplicates for each tablle
    for table in input_tables:
        job_num = job_num +1
        cf.print_run_status(job_num, total_jobs_count, f'{table}_deduplicator.py', os.path.split(input_folder)[1], partner)

        command = ["python", "/app/deduplicator.py", '-f', input_folder, '-of', output_folder , '-t' ]

        # add table list
        command.append(table)

        # Execute the command
        subprocess.run(" ".join(command), shell=True)
###################################################################################################################################
# This function will run the deduplicator script for a single table
###################################################################################################################################

def run_deduplicator(partner, input_folder, output_folder):

    global job_num

    # process duplicates for each tablle
    for table in input_tables:
        job_num = job_num +1
        cf.print_run_status(job_num, total_jobs_count, f'{table}_deduplicator.py', os.path.split(input_folder)[1], partner)

        command = ["python", "/app/deduplicator.py", '-f', input_folder, '-of', output_folder , '-t' ]

        # add table list
        command.append(table)

        # Execute the command
        subprocess.run(" ".join(command), shell=True)

###################################################################################################################################
# This function will run the mapper script for a single table
###################################################################################################################################
def run_mapper(partner, mapper, folder):

    global job_num

    job_num = job_num +1


    partner_settings_path = "partners."+partner+".partner_settings"
    partner_settings = importlib.import_module(partner_settings_path)

    cf.print_run_status(job_num,total_jobs_count, mapper, folder, partner)

    command = ["python", "/app/mapping_scripts/"+partner_settings.mappers_type+"/"+mapper,"-p",partner, '-f', folder ]
    # Execute the command
    subprocess.run(" ".join(command), shell=True)

    

###################################################################################################################################
# This function will run the formatter scripts specified by the user
###################################################################################################################################
def run_formatters_jobs(folder):

    global total_fomatters_count
    if 'all' in input_tables:

        partner_formatters_list = get_partner_formatters_list(input_partner)

        total_fomatters_count = len(partner_formatters_list)

        for formatter  in partner_formatters_list:
            run_formatter(input_partner,formatter,folder)
            
    else:

        for table in input_tables:

            formatter = table.lower()+"_formatter.py"
            run_formatter(input_partner,formatter, folder)


###################################################################################################################################
# This function will run the fixer scripts specified by the user
###################################################################################################################################
def run_fixers_jobs(folder):

    global total_fixers_count
    if 'all' in input_tables:

        partner_fixers_list = get_partner_fixers_list(input_partner)

        non_multi_tables_fixers = [fixer for fixer in partner_fixers_list if "multi_tables" not in fixer.lower()]
        multi_tables_mover_fixers = [fixer for fixer in partner_fixers_list if "multi_tables" in fixer.lower()]
        
        total_fixers_count = len(partner_fixers_list)
        
        # Run all non-multi_tables fixers
        for fixer in non_multi_tables_fixers:
            run_fixer(input_partner, fixer, folder)
        
        # Run the multi_tables_mover
        for fixer in multi_tables_mover_fixers:
            run_fixer(input_partner, fixer, folder)
            
    else:
        # Run all specified fixers except multi_tables_mover
        specified_fixers = [table.lower() for table in input_tables if table.lower() != "multi_tables"]

        for fixer in specified_fixers:
            run_fixer(input_partner, fixer + "_fixer.py", folder)
            
        # If multi_tables_mover was specified
        if "multi_tables" in [table.lower() for table in input_tables]:
            run_fixer(input_partner, "multi_tables_fixer.py", folder)



###################################################################################################################################
# This function will
###################################################################################################################################
def run_dc_checks_jobs(folder):

    global total_fixers_count
    if 'all' in input_dc_checks:

        dc_checks_list = get_dc_checks_list()

        
        
        # Run all non-multi_tables fixers
        for dc_check in dc_checks_list:
            run_dc_check(input_partner, dc_check, folder)
    
            
    else:

        for dc_check in input_dc_checks:

            run_dc_check(input_partner, dc_check, folder)


###################################################################################################################################
# This function will 
###################################################################################################################################

def run_dc_check(partner, dc_check, folder):

    global job_num

    job_num = job_num +1
    cf.print_run_status(job_num,total_jobs_count,dc_check, folder, partner)


    command = ["python", "/app/dc_checks_scripts/"+dc_check+'.py', '-f', folder , '-p', partner]
    # Execute the command
    subprocess.run(" ".join(command), shell=True)

###################################################################################################################################
# This function will run the deduplicators script
###################################################################################################################################

def run_deduplicator_jobs(folder):

    formatted_files_list_path = '/app/partners/'+input_partner.lower()+'/data/'+folder+'/formatter_output/'


    global total_uploads_count
    global job_num

    if 'all' in input_tables:

        formatted_files_list = get_partner_formatted_files_list(formatted_files_list_path)
        # total_uploads_count = len(partner_uploads_list)

        

        for file_name  in formatted_files_list:

            table_name = file_name.replace('formatted_',"").replace('.csv',"")

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"deduplicating {file_name}", folder, input_partner)

            cf.deduplicate(
                         partner = input_partner,
                         file_name=table_name.upper(), 
                         table_name = table_name.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )
    else:

        for table in input_tables:
            formatted_table_name = "formatted_"+table.lower()+".csv"

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"deduplicating {formatted_table_name}", folder, input_partner)


            cf.deduplicate(
                         partner = input_partner,
                         file_name=table.upper(),
                         table_name = table.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )

###################################################################################################################################
# This function will run the mapper scripts specified by the user
###################################################################################################################################

def run_mappers_jobs(folder):

    global total_mappers_count

    if 'all' in input_tables:

        partner_mappers_list = get_partner_mappers_list(input_partner.lower())
        # total_mappers_count = len(partner_mappers_list)


        for mapper  in partner_mappers_list:
            run_mapper(input_partner,mapper, folder)
    else:

        for table in input_tables:
            mapper = table.lower()+"_mapper.py"
            run_mapper(input_partner,mapper, folder)


###################################################################################################################################
# This function will run upload for all tables to the data base specified by the user 
###################################################################################################################################

def run_mapping_gap_jobs(folder):

    formatted_files_list_path = '/app/partners/'+input_partner.lower()+'/data/'+folder+'/formatter_output/'


    global total_uploads_count
    global job_num

    if 'all' in input_tables:

        formatted_files_list = get_partner_formatted_files_list(formatted_files_list_path)
        # total_uploads_count = len(partner_uploads_list)

        

        for file_name  in formatted_files_list:

            table_name = file_name.replace('formatted_',"").replace('.csv',"")

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"mapping gap for {file_name}", folder, input_partner)

            cf.get_mapping_gap(
                         partner = input_partner,
                         file_name=table_name.upper(), 
                         table_name = table_name.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )
    else:

        for table in input_tables:
            formatted_table_name = "formatted_"+table.lower()+".csv"

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"mapping gap for {formatted_table_name}", folder, input_partner)


            cf.get_mapping_gap(
                         partner = input_partner,
                         file_name=table.upper(),
                         table_name = table.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )

###################################################################################################################################
# This function will run upload for all tables to the data base specified by the user 
###################################################################################################################################

def run_monthly_count_jobs(folder):


    formatted_files_list_path = '/app/partners/'+input_partner.lower()+'/data/'+folder+'/formatter_output/'

    global total_uploads_count
    global job_num

    if 'all' in input_tables:

        formatted_files_list = get_partner_formatted_files_list(formatted_files_list_path)
        # total_uploads_count = len(partner_uploads_list)

        

        for file_name  in formatted_files_list:

            table_name = file_name.replace('formatted_',"").replace('.csv',"")

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"monthly counts for {file_name}", folder, input_partner)

            cf.get_mothly_counts(
                         partner = input_partner,
                         file_name=table_name.upper(), 
                         table_name = table_name.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )
    else:

        for table in input_tables:
            formatted_table_name = "formatted_"+table.lower()+".csv"

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"monthly counts for {formatted_table_name}", folder, input_partner)


            cf.get_mothly_counts(
                         partner = input_partner,
                         file_name=table.upper(),
                         table_name = table.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )

###################################################################################################################################
# This function will generate a mapping report as a form of an excel file
###################################################################################################################################

def run_mapping_report_job(folder):

    mapping_gap_output_folder_path = '/app/partners/'+input_partner.lower()+'/data/'+folder+'/mapping_gap_output/'



    global total_uploads_count 
    global job_num

    job_num = job_num +1

    cf.print_run_status(job_num,total_jobs_count, f"Generating the mapping report ", folder, input_partner)

    cf.generate_mapping_report(

        mapping_gap_output_folder_path = mapping_gap_output_folder_path,
        folder = folder,
        partner= input_partner
    )




###################################################################################################################################
# This function will run upload for all tables to the data base specified by the user 
###################################################################################################################################

def run_upload_jobs(folder):



        fixed_files_path = '/app/partners/'+input_partner.lower()+'/data/'+folder+'/fixer_output/'

        global total_uploads_count
        global job_num


        partner_settings_path = "partners."+input_partner.lower()+".partner_settings"
        partner_settings = importlib.import_module(partner_settings_path)

        mappers_type =  partner_settings.mappers_type

        if 'all' in input_tables:

            partner_uploads_list = get_partner_uploads_list(fixed_files_path)

            

            for file_name  in partner_uploads_list:

                table_name = file_name.replace('fixed_',"").replace('.csv',"")

                job_num = job_num +1
                cf.print_run_status(job_num,total_jobs_count, f"uploading to {input_db.upper()}.OVID_{folder.upper()}.{table_name.upper()}", folder, input_partner)


                # schemas = cf.get_schemas( table_name.upper(), fixed_files_path)

                cf.db_upload(db= input_db,
                            db_server=input_db_server,
                            file_name=file_name, 
                            table_name = table_name.upper(),
                            file_path= fixed_files_path, 
                            folder_name = folder,
                            db_type = input_db_type,
                            upload_type = input_upload_type,
                            merge_db = input_merge_db,
                            merge_schema = input_merge_schema,
                            )

            cf.print_run_status(job_num,total_jobs_count, f"uploading CDM complementary tables", folder, input_partner)
            
            job_num = job_num +1




            cf.db_upload_cdm_complementary_tables(db= input_db,
                                        db_server=input_db_server,
                                        folder_name = folder,
                                        db_type = input_db_type,
                                        mappers_type =mappers_type
                                        )
        else:

            for table in input_tables:
                fixed_table_name = "fixed_"+table.lower()+".csv"


                if "cdm_complementary" not in table.lower():


                    if os.path.exists(fixed_files_path+fixed_table_name):

                        job_num = job_num +1
                        cf.print_run_status(job_num,total_jobs_count, f"uploading to {input_db.upper()}.OVID_{folder.upper()}.{table.upper()}", folder, input_partner)


                        # schemas = cf.get_schemas(table.upper(),fixed_files_path)


                        cf.db_upload(db= input_db,
                                    db_server=input_db_server,
                                    # schemas=schemas,
                                    file_name=fixed_table_name, 
                                    table_name = table.upper(),
                                    file_path= fixed_files_path,
                                    folder_name = folder,
                                    db_type = input_db_type,
                                    upload_type = input_upload_type,
                                    merge_db = input_merge_db,
                                    merge_schema = input_merge_schema,
                                    )
                        

                    else:

                            cf.print_failure_message(
                                                    folder  = folder,
                                                    partner = input_partner,
                                                    job     = 'upload '+ table.lower()  ,
                                                    text    =  f"{fixed_table_name} Path does not exist!!!!"
                                                    )





                else:

                        cf.print_run_status(job_num,total_jobs_count, f"uploading CDM complementary tables", folder, input_partner)
                                                
                        job_num = job_num +1

                        cf.db_upload_cdm_complementary_tables(db= input_db,
                                        db_server=input_db_server,
                                        folder_name = folder,
                                        db_type = input_db_type,
                                        mappers_type =mappers_type
                                        )




###################################################################################################################################
# checking for correct input
###################################################################################################################################

if  not cf.valid_partner_name(input_partner):

    logger.error(f"Unrecognized partner: {input_partner}")
    print("Error: Unrecognized partner "+input_partner+" !!!!!")
    sys.exit()



for job in input_jobs:
   
    if job not in VALID_ETL_JOBS:

        logger.error(f"Invalid job specified: {job}")
        print(job+ " is not a valid job!!!!!! Please enter a valid job name eg. -j format or -j deduplicate or -j map, or -j all")
        
        sys.exit()


if 'format' in input_jobs or 'all' in input_jobs:

    if input_data_folders == "" or input_data_folders == None:

        logger.error("No data folder specified")
        print("Please enter a valid data folder name !!!!!")
        sys.exit()
    else:


      
        for folder in input_data_folders:



            path = f"/data/{folder}"

            if not os.path.exists(path):

                logger.error(f"Path does not exist: {path}")
                print("Path  "+path+" does not exits !!!!!!!")
                sys.exit()



###################################################################################################################################
# Getting Jobs count
###################################################################################################################################

global total_jobs_count

folders_count = len(input_data_folders)


if 'all' in  input_jobs:
    jobs_count = 4
else:
    jobs_count = len(input_jobs)

try:
    
    if 'all' in input_tables:
        tables_count = len(get_partner_formatters_list(input_partner))
    else:
        tables_count = len(input_tables)
except:
    pass

total_jobs_count = '**' #folders_count * jobs_count * tables_count


###################################################################################################################################
# This function will run the dv_mapper script for tables specified by the user 
###################################################################################################################################

def run_dv_mapper_jobs(folder ):
    global job_num
    total_jobs_count = len(input_tables)
    job_num = 0


    
    dv_mapping_table_path = f'/app/partners/{input_partner}/data/dv_match/{folder}/DV_ORIGINAL_PATID_MAPPING.csv'
    dv_mapper_output_path = f'/app/partners/{input_partner}/data/dv_mapper_output/{folder}/'
    fixed_table_path      = f'/app/partners/{input_partner}/data/fixer_output/{folder}/'
   
    for table_name in input_tables:
        job_num += 1
        cf.print_run_status(job_num, total_jobs_count, f" mapping {table_name} to Datavant ids", "", "")


        cf.dv_match(partner_name=input_partner,
                    folder=folder,
                    dv_mapping_table_path=dv_mapping_table_path,
                    fixed_table_path = fixed_table_path,
                    table_name=table_name,
                    output_path=dv_mapper_output_path,
                    dv_mapper_output_path=dv_mapper_output_path
            
                )



###################################################################################################################################
# This function will run upload for all tables to the data base specified by the user 
###################################################################################################################################

def run_monthly_counts_jobs(folder):


    formatted_files_list_path = '/app/partners/'+input_partner.lower()+'/data/'+folder+'/formatter_output/'

    global total_uploads_count
    global job_num

    if 'all' in input_tables:

        formatted_files_list = get_partner_formatted_files_list(formatted_files_list_path)
        # total_uploads_count = len(partner_uploads_list)

        

        for file_name  in formatted_files_list:

            table_name = file_name.replace('formatted_',"").replace('.csv',"")

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"monthly counts for {file_name}", folder, input_partner)

            cf.get_mothly_counts(
                         partner = input_partner,
                         file_name=table_name.upper(), 
                         table_name = table_name.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )
    else:

        for table in input_tables:
            formatted_table_name = "formatted_"+table.lower()+".csv"

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"monthly counts for {formatted_table_name}", folder, input_partner)


            cf.get_mothly_counts(
                         partner = input_partner,
                         file_name=table.upper(),
                         table_name = table.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )



###################################################################################################################################
# This function will run upload for all tables to the data base specified by the user 
###################################################################################################################################

def run_distinct_values_jobs(folder):


    formatted_files_list_path = '/app/partners/'+input_partner.lower()+'/data/'+folder+'/formatter_output/'

    global total_uploads_count
    global job_num

    if 'all' in input_tables:

        formatted_files_list = get_partner_formatted_files_list(formatted_files_list_path)
        # total_uploads_count = len(partner_uploads_list)

        

        for file_name  in formatted_files_list:

            table_name = file_name.replace('formatted_',"").replace('.csv',"")

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"distinct values for {file_name}", folder, input_partner)

            cf.get_distinct_values(
                         partner = input_partner,
                         file_name=table_name.upper(), 
                         table_name = table_name.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )
    else:

        for table in input_tables:
            formatted_table_name = "formatted_"+table.lower()+".csv"

            job_num = job_num +1
            cf.print_run_status(job_num,total_jobs_count, f"distinct values for {formatted_table_name}", folder, input_partner)


            cf.get_distinct_values(
                         partner = input_partner,
                         file_name=table.upper(),
                         table_name = table.upper(),
                         file_path= formatted_files_list_path,
                         folder_name = folder
                          )
###################################################################################################################################
# submitting the jobs to be run
###################################################################################################################################
 

for folder in input_data_folders:

    logger.info(f"Processing folder: {folder}")

    if  'all' in input_jobs:

        logger.info("Running all ETL jobs: format -> deduplicate -> map -> fix -> upload")
        run_formatters_jobs(folder)
        run_deduplicator_jobs(folder)
        run_mappers_jobs(folder)
        run_fixers_jobs(folder)
        run_upload_jobs(folder)

    else :


        if  'format' in input_jobs:
            logger.info("Starting FORMAT job")
            run_formatters_jobs(folder)     

        if  'deduplicate' in input_jobs:
            logger.info("Starting DEDUPLICATE job")
            run_deduplicator_jobs(folder)

        if  'map' in input_jobs:
            logger.info("Starting MAP job")
            run_mappers_jobs(folder)

        if  'fix' in input_jobs:
            logger.info("Starting FIX job")
            run_fixers_jobs(folder)


        if  'upload' in input_jobs:
            logger.info("Starting UPLOAD job")
            run_upload_jobs(folder)


    logger.info(f"Completed processing folder: {folder}")


        

    

logger.info("=" * 60)
logger.info("ETL Pipeline completed successfully")
logger.info("=" * 60)

print("""

                                                    ▀▀█▀▀ ░█ ░█ ░█▀▀▀ 　 ░█▀▀▀ ░█▄ ░█ ░█▀▀▄ 　 █ 
                                                     ░█   ░█▀▀█ ░█▀▀▀ 　 ░█▀▀▀ ░█░█░█ ░█ ░█ 　 ▀ 
                                                     ░█   ░█ ░█ ░█▄▄▄ 　 ░█▄▄▄ ░█  ▀█ ░█▄▄▀ 　 ▄
            
    """)
             