
from commonFunctions import CommonFuncitons as cf
import importlib
import sys
import os
import subprocess
import fnmatch
import pyspark
from pyspark.sql import SparkSession
import argparse
from datetime import datetime
import pytz
from settings import  VALID_ANALYSIS_JOBS
from ovid_logging import setup_logging, get_logger, log_job_start, log_job_end, print_pipeline_summary

from PIL import Image

# Ensure /app/common is on PYTHONPATH for all subprocesses.
# spark-submit --py-files distributes common.zip via temp dirs that may be
# cleaned up after spark.stop(), breaking imports in child processes.
_pypath = os.environ.get('PYTHONPATH', '')
if '/app/common' not in _pypath:
    os.environ['PYTHONPATH'] = '/app/common' + (':' + _pypath if _pypath else '')

master_url = os.environ["SPARK_URL"].strip().replace('"', '')

# Extract timestamp from master URL (format: spark://ovid-master-YYYYMMDD-HHMMSS:7077)
master_name = master_url.replace('spark://', '').replace(':7077', '')
timestamp = master_name.replace('ovid-master-', '')


spark = SparkSession.builder.master(master_url).appName("Ovid").getOrCreate()


# spark = SparkSession.builder.master(os.environ["SPARK_URL"]).appName("Ovid").getOrCreate()
spark.sparkContext.setLogLevel('OFF')
spark.stop()


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
parser.add_argument("-fld", "--field", nargs="+")



args = parser.parse_args()

input_jobs = args.job
input_partner = args.partner.lower()
input_tables = args.table
input_db = args.database
input_db_server = args.db_server
input_data_folders = args.data_folder
input_db_type = args.database_type
input_dc_checks = args.dc_check
input_fields = args.field if args.field is not None else ['all']

# Print startup summary
print_pipeline_summary("OVID Analysis Pipeline", [
    ("Partner",   input_partner),
    ("Folder(s)", ', '.join(input_data_folders) if input_data_folders else 'N/A'),
    ("Jobs",      ', '.join(input_jobs) if input_jobs else 'N/A'),
    ("Tables",    ', '.join(input_tables) if input_tables else 'N/A'),
    ("Fields",    ', '.join(input_fields) if input_fields else 'all'),
    ("Instance",  timestamp),
])
# Flush stdout immediately so the banner reaches the host-side log streamer
# even when the parent harness (opencode/ansible/CI) buffers or terminates
# stdout before the live-display thread connects.
sys.stdout.flush()

# Initialize logging with timestamp and partner
logger = setup_logging(timestamp=timestamp, partner=input_partner)
logger.info(f"Analysis Pipeline started - Partner: {input_partner}, Jobs: {input_jobs}, Tables: {input_tables}")



# cf =CommonFuncitons(input_partner)

logger.info("=" * 60)
logger.info("Starting Analysis job execution")
logger.info("=" * 60)

for folder in input_data_folders:
    logger.info(f"Processing folder: {folder}")
    if 'all' in input_jobs:
        logger.info("Running all analysis jobs: format -> monthly_counts -> distinct_values")

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
    # Execute the command (logging handled in-process by print_with_style)
    cf.run_subprocess(" ".join(command), shell=True)



###################################################################################################################################
# This function will run the formatter script for a single table
###################################################################################################################################

def run_fixer(partner, fixer, folder):

    global job_num

    job_num = job_num +1
    
    # Set work directory for sync files
    import os
    work_dir = f"/app/partners/{partner}/data/{folder}"
    
    # Print status with subprocess_mode=True to avoid race condition
    cf.print_run_status(job_num, total_jobs_count, fixer, folder, partner, subprocess_mode=True)
    
    # Set up sync files (subprocess_running is already True from print_run_status)
    cf.pause_timer_for_subprocess(work_dir=work_dir)

    command = ["python", "/app/partners/"+partner+"/fixer_scripts/"+fixer, '-f', folder]
    
    # Pass environment variables to subprocess
    env = os.environ.copy()
    if cf._overall_start_time:
        env['OVID_OVERALL_START_TIME'] = cf._overall_start_time.isoformat()
    env['OVID_WORK_DIR'] = work_dir
    # Ensure /app/common is on PYTHONPATH so subprocesses can import commonFunctions
    existing_pypath = env.get('PYTHONPATH', '')
    if '/app/common' not in existing_pypath:
        env['PYTHONPATH'] = '/app/common' + (':' + existing_pypath if existing_pypath else '')
    
    # Execute the command (output captured to log, not terminal)
    cf.run_subprocess(command, env=env)
    
    # Resume the timer after fixer subprocess completes
    cf.resume_timer_after_subprocess()

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

        # Execute the command (logging handled in-process by print_with_style)
        cf.run_subprocess(" ".join(command), shell=True)
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

        # Execute the command (logging handled in-process by print_with_style)
        cf.run_subprocess(" ".join(command), shell=True)

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
    # Execute the command (logging handled in-process by print_with_style)
    cf.run_subprocess(" ".join(command), shell=True)

    

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
    # Execute the command (logging handled in-process by print_with_style)
    cf.run_subprocess(" ".join(command), shell=True)

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

        if 'all' in input_tables:

            partner_uploads_list = get_partner_uploads_list(fixed_files_path)

            

            for file_name  in partner_uploads_list:

                table_name = file_name.replace('fixed_',"").replace('.csv',"")

                job_num = job_num +1
                # Extract date from folder (e.g., "BND/20251224" -> "20251224")
                schema_date = folder.split('/')[-1] if '/' in folder else folder
                cf.print_run_status(job_num,total_jobs_count, f"uploading to {input_db.upper()}.OVID_{schema_date.upper()}.{table_name.upper()}", folder, input_partner)


                schemas = cf.get_schemas( table_name.upper(), fixed_files_path)

                cf.db_upload(db= input_db,
                            db_server=input_db_server,
                            schemas=schemas,
                            file_name=file_name, 
                            table_name = table_name.upper(),
                            file_path= fixed_files_path, 
                            folder_name = folder,
                            db_type = input_db_type
                            )

            cf.print_run_status(job_num,total_jobs_count, f"uploading CDM complementary tables", folder, input_partner)
            
            job_num = job_num +1

            cf.db_upload_cdm_complementary_tables(db= input_db,
                                        db_server=input_db_server,
                                        folder_name = folder,
                                        db_type = input_db_type
                                        )
        else:

            for table in input_tables:
                fixed_table_name = "fixed_"+table.lower()+".csv"


                if "cdm_complementary" not in table.lower():


                    if os.path.exists(fixed_files_path+fixed_table_name):

                        job_num = job_num +1
                        # Extract date from folder (e.g., "BND/20251224" -> "20251224")
                        schema_date = folder.split('/')[-1] if '/' in folder else folder
                        cf.print_run_status(job_num,total_jobs_count, f"uploading to {input_db.upper()}.OVID_{schema_date.upper()}.{table.upper()}", folder, input_partner)


                        schemas = cf.get_schemas(table.upper(),fixed_files_path)


                        cf.db_upload(db= input_db,
                                    db_server=input_db_server,
                                    schemas=schemas,
                                    file_name=fixed_table_name, 
                                    table_name = table.upper(),
                                    file_path= fixed_files_path,
                                    folder_name = folder,
                                    db_type = input_db_type
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
                                        db_type = input_db_type
                                        )




###################################################################################################################################
# checking for correct input
###################################################################################################################################

if  not cf.valid_partner_name(input_partner):

    logger.error(f"Unrecognized partner: {input_partner}")
    print("Error: Unrecognized partner "+input_partner+" !!!!!")
    sys.exit()



for job in input_jobs:
   
    if job not in VALID_ANALYSIS_JOBS:

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
    jobs_count = 5  # format, deduplicate, map, fix, upload
else:
    jobs_count = len(input_jobs)

try:
    
    if 'all' in input_tables:
        tables_count = len(get_partner_formatters_list(input_partner))
    else:
        tables_count = len(input_tables)
except:
    tables_count = 1

total_jobs_count = folders_count * jobs_count * tables_count


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
# This function will compute distinct values directly on raw input files in /data/{folder}/.
# Unlike run_distinct_values_jobs which operates on formatter_output, this targets the
# untouched OMOP-format (or any csv/tsv) source files. It supports glob patterns for
# both file names (-t) and field names (-fld), enriches *_concept_id columns with
# concept_name/code/vocabulary_id from a global OMOP CONCEPT.csv if present, and
# generates a comprehensive Excel report at the end.
###################################################################################################################################

def run_raw_distinct_values_jobs(folder: str) -> None:
    """Run distinct-value profiling on raw input files in /data/{folder}/.

    Files are matched by user-supplied -t glob patterns (or 'all'). Every
    file matching a single explicit pattern is grouped into one logical
    table -- so ``-t measurement`` profiles all measurement-named files
    (e.g. chunked ``measurement_1.csv``, ``measurement_2.csv``) as a
    single dataset producing one section in the PDF and one output
    subdirectory. The ``-t all`` shortcut keeps the historical
    one-section-per-file rendering. Fields within each group are matched
    by ``-fld`` glob patterns (or ``'all'``). Outputs land at
    ``partners/{partner}/data/{folder}/raw_distinct_values/{group_name}/``,
    plus a single comprehensive PDF report named
    ``{partner}_{folder_label}_raw_distinct_values_report.pdf``.

    Args:
        folder: Data delivery folder identifier (e.g. 'UCI/20251121').
    """
    global job_num

    raw_input_dir = f'/data/{folder}/'
    concept_table_path = '/app/common/omop_cdm/CONCEPT.csv'

    matched_groups = cf._raw_dv_match_files_grouped(raw_input_dir, input_tables)

    if not matched_groups:
        logger.warning(
            f"[raw_distinct_values] no files in {raw_input_dir} matched table patterns {input_tables}"
        )
        return

    total_files = sum(len(g["files"]) for g in matched_groups)
    logger.info(
        f"[raw_distinct_values] matched {total_files} file(s) in {len(matched_groups)} "
        f"group(s) under {raw_input_dir}"
    )

    file_results = []
    for group in matched_groups:
        job_num = job_num + 1
        group_name = group["group_name"]
        group_files = group["files"]
        group_pattern = group.get("pattern", "")
        cf.print_run_status(
            job_num,
            total_jobs_count,
            f"raw distinct values for '{group_name}' ({len(group_files)} file(s))",
            folder,
            input_partner,
        )

        result = cf.get_raw_distinct_values(
            partner=input_partner,
            folder_name=folder,
            group_name=group_name,
            file_paths=group_files,
            field_patterns=input_fields,
            concept_table_path=concept_table_path,
        )
        # Surface the originating pattern in the result so the report's
        # _group_meta.csv (and any future diagnostic) can show it.
        if group_pattern:
            result["pattern"] = group_pattern
        file_results.append(result)

    # Build the PDF report after all per-group CSVs are written.
    job_num = job_num + 1
    cf.print_run_status(
        job_num,
        total_jobs_count,
        "generating raw distinct values PDF report",
        folder,
        input_partner,
    )
    cf.generate_raw_distinct_values_report(
        partner=input_partner,
        folder_name=folder,
        file_results=file_results,
    )

    # Diagnostic: warn when any -fld pattern matched no column in any
    # profiled group. Helps catch typos like 'procedure_concept_idd'. The
    # 'all' pattern is excluded from this check because it always trivially
    # matches.
    try:
        all_profiled_columns = {
            str(c["column"]).lower()
            for fr in file_results
            for c in fr.get("columns", [])
        }
        unmatched: list = []
        for pat in input_fields or []:
            if str(pat).lower() == "all":
                continue
            pat_lower = str(pat).lower()
            if not any(
                fnmatch.fnmatch(c, pat_lower) for c in all_profiled_columns
            ):
                unmatched.append(pat)
        if unmatched:
            logger.warning(
                f"[raw_distinct_values] field pattern(s) matched no column "
                f"in any profiled group: {', '.join(unmatched)}"
            )
    except Exception as warn_exc:
        logger.warning(
            f"[raw_distinct_values] failed to compute unmatched-field warning: {warn_exc}"
        )
###################################################################################################################################
# submitting the jobs to be run
###################################################################################################################################

print("""
      

                            ▄█  ░█▀▀▀ ░█       ░█▀▀█ ░█ ░█  █▀▀█ ░█▄ ░█ ░█▀▀█ ░█▀▀▀ ▀█▀ ░█▄ ░█  █▀▀█ ▀▀█▀▀ ░█▀▀▀█ ░█▀▀█
                             █  ░█▀▀▀ ░█    ▀▀ ░█    ░█▀▀█ ░█▄▄█ ░█░█░█ ░█ ▄▄ ░█▀▀▀ ░█  ░█░█░█ ░█▄▄█  ░█   ░█  ░█ ░█▄▄▀
                            ▄█▄ ░█    ░█▄▄█    ░█▄▄█ ░█ ░█ ░█ ░█ ░█  ▀█ ░█▄▄█ ░█▄▄▄ ▄█▄ ░█  ▀█ ░█ ░█  ░█   ░█▄▄▄█ ░█ ░█
  
      
        """)

for folder in input_data_folders:

    if  'all' in input_jobs:

        run_formatters_jobs(folder)
        run_monthly_counts_jobs(folder)
        run_distinct_values_jobs(folder)
        run_raw_distinct_values_jobs(folder)

    else :


        if  'format' in input_jobs:
            logger.info("Starting FORMAT job")
            run_formatters_jobs(folder)

        if  'monthly_counts' in input_jobs:
            logger.info("Starting MONTHLY_COUNTS job")
            run_monthly_counts_jobs(folder)

        if  'distinct_values' in input_jobs:
            logger.info("Starting DISTINCT_VALUES job")
            run_distinct_values_jobs(folder)

        if  'raw_distinct_values' in input_jobs:
            logger.info("Starting RAW_DISTINCT_VALUES job")
            run_raw_distinct_values_jobs(folder)


print("""
      

                                                    ▀▀█▀▀ ░█ ░█ ░█▀▀▀ 　 ░█▀▀▀ ░█▄ ░█ ░█▀▀▄ 　 █ 
                                                     ░█   ░█▀▀█ ░█▀▀▀ 　 ░█▀▀▀ ░█░█░█ ░█ ░█ 　 ▀ 
                                                     ░█   ░█ ░█ ░█▄▄▄ 　 ░█▄▄▄ ░█  ▀█ ░█▄▄▀ 　 ▄
            
    
      """)
             
             