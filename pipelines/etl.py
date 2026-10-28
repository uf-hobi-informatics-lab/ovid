
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
from ovid_logging import setup_logging, get_logger, log_job_start, log_job_end, log_table_processing, print_pipeline_summary

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

# Print startup summary
print_pipeline_summary("OVID ETL Pipeline", [
    ("Partner",     input_partner),
    ("Folder(s)",   ', '.join(input_data_folders) if input_data_folders else 'N/A'),
    ("Stages",      ', '.join(input_jobs) if input_jobs else 'N/A'),
    ("Tables",      ', '.join(input_tables) if input_tables else 'N/A'),
    ("Database",    input_db or 'N/A'),
    ("DB Type",     input_db_type or 'N/A'),
    ("DB Server",   input_db_server or 'N/A'),
    ("Upload Type", input_upload_type or 'N/A'),
    ("Instance",    timestamp),
])
# Flush stdout immediately so the banner reaches the host-side log streamer
# even when the parent harness (opencode/ansible/CI) buffers or terminates
# stdout before the live-display thread connects.
sys.stdout.flush()

# Initialize logging with timestamp and partner
logger = setup_logging(timestamp=timestamp, partner=input_partner)
logger.info(f"ETL Pipeline started - Partner: {input_partner}, Jobs: {input_jobs}, Tables: {input_tables}")


# Clear stale failure queue and flag from previous runs to prevent
# old failure messages from being displayed in this run
cf.clear_stale_sync_files()


# cf =CommonFuncitons(input_partner)

logger.info("=" * 60)
logger.info("Starting ETL job execution")
logger.info("=" * 60)

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
    
    # Set work directory for sync files
    import os
    work_dir = f"/app/partners/{partner}/data/{folder}"
    
    # Print status with subprocess_mode=True to avoid race condition
    cf.print_run_status(job_num, total_jobs_count, formatter, folder, partner, subprocess_mode=True)
    
    # Set up sync files (subprocess_running is already True from print_run_status)
    cf.pause_timer_for_subprocess(work_dir=work_dir)

    command = ["python", "/app/partners/"+partner+"/formatting_scripts/"+formatter, '-f', folder , '-p', partner  ]
    
    # Pass environment variables to subprocess
    env = os.environ.copy()
    if cf._overall_start_time:
        env['OVID_OVERALL_START_TIME'] = cf._overall_start_time.isoformat()
    env['OVID_WORK_DIR'] = work_dir
    # Ensure /app/common is on PYTHONPATH so subprocesses can import commonFunctions
    # (spark-submit --py-files only adds common.zip to the driver's sys.path;
    #  the temp copy may be cleaned up by spark.stop() before subprocesses run)
    existing_pypath = env.get('PYTHONPATH', '')
    if '/app/common' not in existing_pypath:
        env['PYTHONPATH'] = '/app/common' + (':' + existing_pypath if existing_pypath else '')
    
    # Execute the command (output captured to log, not terminal)
    cf.run_subprocess(command, env=env)
    
    # Resume the timer after formatter subprocess completes
    cf.resume_timer_after_subprocess()



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

        # Execute the command (output captured to log, not terminal)
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

        # Execute the command (output captured to log, not terminal)
        cf.run_subprocess(" ".join(command), shell=True)

###################################################################################################################################
# This function will run the mapper script for a single table
###################################################################################################################################
def run_mapper(partner, mapper, folder):

    global job_num

    job_num = job_num +1

    # Set work directory for sync files
    import os
    work_dir = f"/app/partners/{partner}/data/{folder}"

    partner_settings_path = "partners."+partner+".partner_settings"
    partner_settings = importlib.import_module(partner_settings_path)

    # Print status with subprocess_mode=True to avoid race condition
    cf.print_run_status(job_num, total_jobs_count, mapper, folder, partner, subprocess_mode=True)
    
    # Set up sync files (subprocess_running is already True from print_run_status)
    cf.pause_timer_for_subprocess(work_dir=work_dir)

    command = ["python", "/app/mapping_scripts/"+partner_settings.mappers_type+"/"+mapper,"-p",partner, '-f', folder ]
    
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
    
    # Resume the timer after mapper subprocess completes
    cf.resume_timer_after_subprocess()

    

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
    
    # Set work directory for sync files
    import os
    work_dir = f"/app/partners/{partner}/data/{folder}"
    
    # Print status with subprocess_mode=True to avoid race condition
    cf.print_run_status(job_num, total_jobs_count, dc_check, folder, partner, subprocess_mode=True)
    
    # Set up sync files (subprocess_running is already True from print_run_status)
    cf.pause_timer_for_subprocess(work_dir=work_dir)

    command = ["python", "/app/dc_checks_scripts/"+dc_check+'.py', '-f', folder , '-p', partner]
    
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
    
    # Resume the timer after dc_check subprocess completes
    cf.resume_timer_after_subprocess()

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

            if len(partner_uploads_list) == 0:
                logger.warning(f"No fixed_*.csv files found in {fixed_files_path} - nothing to upload!")
                print(f"WARNING: No fixed_*.csv files found in {fixed_files_path}")
                print("Please ensure the fix job has been run before upload.")

            for file_name  in partner_uploads_list:

                table_name = file_name.replace('fixed_',"").replace('.csv',"")

                job_num = job_num +1
                # Extract date from folder (e.g., "PARTNER/20251224" -> "20251224")
                schema_date = folder.split('/')[-1] if '/' in folder else folder
                cf.print_run_status(job_num,total_jobs_count, f"uploading to {input_db.upper()}.OVID_{schema_date.upper()}.{table_name.upper()}", folder, input_partner)


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
                            partner_name = input_partner,
                            )

            cf.print_run_status(job_num,total_jobs_count, f"uploading CDM complementary tables", folder, input_partner)
            
            job_num = job_num +1




            cf.db_upload_cdm_complementary_tables(db= input_db,
                                        db_server=input_db_server,
                                        folder_name = folder,
                                        db_type = input_db_type,
                                        mappers_type =mappers_type,
                                        upload_type = input_upload_type,
                                        merge_db = input_merge_db,
                                        merge_schema = input_merge_schema
                                        )
        else:

            uploaded_count = 0
            skipped_count = 0
            skipped_tables = []

            for table in input_tables:
                fixed_table_name = "fixed_"+table.lower()+".csv"


                if "cdm_complementary" not in table.lower():


                    if os.path.exists(fixed_files_path+fixed_table_name):

                        job_num = job_num +1
                        # Extract date from folder (e.g., "PARTNER/20251224" -> "20251224")
                        schema_date = folder.split('/')[-1] if '/' in folder else folder
                        cf.print_run_status(job_num,total_jobs_count, f"uploading to {input_db.upper()}.OVID_{schema_date.upper()}.{table.upper()}", folder, input_partner)


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
                                    partner_name = input_partner,
                            
                                    )
                        uploaded_count += 1

                    else:
                            skipped_count += 1
                            skipped_tables.append(table)
                            logger.warning(f"Upload skipped for table '{table}': file {fixed_table_name} not found at {fixed_files_path}")
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
                                        mappers_type =mappers_type,
                                        upload_type = input_upload_type,
                                        merge_db = input_merge_db,
                                        merge_schema = input_merge_schema
                                        )

            if skipped_count > 0:
                logger.warning(f"Upload summary: {uploaded_count} tables uploaded, {skipped_count} tables skipped: {skipped_tables}")
                print(f"\nWARNING: {skipped_count} out of {len(input_tables)} tables were skipped because fixed files were not found.")
                print(f"Skipped tables: {', '.join(skipped_tables)}")
                print(f"Expected path: {fixed_files_path}")
                print("Please ensure the fix job has been run for these tables before upload.\n")

        # After every table for this batch has merged, re-synchronise ENC_TYPE
        # and ADMIT_DATE in the merged DIAGNOSIS/PROCEDURES tables against the
        # merged ENCOUNTER table. The per-table INSERT ... WHERE NOT EXISTS merge
        # can carry forward stale replicated values in the child tables while a
        # corrected ENCOUNTER row wins, reproducing PCORnet DC 1.10. Running this
        # once as a final pass avoids depending on ENCOUNTER being uploaded
        # before its children. Only applies to Snowflake merge uploads.
        if input_upload_type == 'merge' and input_db_type and input_db_type.upper() == 'SF':

            schema_folder_name = folder.split('/')[1] if '/' in folder else folder
            merged_schema_name = f"OVID_{schema_folder_name}_merged_with_{input_merge_db}_{input_merge_schema}"

            cf.print_run_status(
                job_num, total_jobs_count,
                f"reconciling replication (ENC_TYPE/ADMIT_DATE) in {merged_schema_name}",
                folder, input_partner,
            )

            try:
                cf.reconcile_replication_post_merge(
                    db=input_db,
                    db_server=input_db_server,
                    merged_schema_name=merged_schema_name,
                )
            except Exception as e:
                logger.error(
                    f"Post-merge replication reconciliation failed for {merged_schema_name}: {e}",
                    exc_info=True,
                )
                cf.print_failure_message(
                    folder=folder,
                    partner=input_partner,
                    job='reconcile_replication_post_merge',
                    text=str(e),
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


if 'format' in input_jobs or 'all' in input_jobs or 'upload' in input_jobs:

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

            # Validate fixer_output path exists when running upload standalone (without fix or earlier stages)
            if 'upload' in input_jobs and 'fix' not in input_jobs and 'all' not in input_jobs:
                fixer_output_path = '/app/partners/'+input_partner.lower()+'/data/'+folder+'/fixer_output/'
                if not os.path.exists(fixer_output_path):
                    logger.error(f"Fixer output path does not exist: {fixer_output_path}")
                    print(f"Error: fixer_output directory does not exist at {fixer_output_path}")
                    print("Please run the fix job before upload, or verify the data path.")
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
# submitting the jobs to be run
###################################################################################################################################

# Track whether any phase raised a Python exception that escaped the
# helpers' own try/except blocks. Combined with cf._check_failure_flag()
# (set by print_failure_message() when a subprocess writes a failure
# message), this lets us decide the parent script's exit code at the
# end -- so the scheduler's `result.returncode` correctly reflects
# whether the batch actually completed end-to-end.
#
# Why this exists: a past production batch demonstrated a class of
# silent-PROCESSED bugs. Helpers like cf.deduplicate() catch
# exceptions internally (commonFunctions.py:867) and call
# print_with_style(...) which writes only to the terminal -- the
# parent ETL pipeline keeps looping through dedup/map/fix/upload
# even though every call is failing. Without this guard the parent
# exits with returncode 0 and the scheduler marks the batch
# PROCESSED in OVID.SCHEDULER.data_batch -- a false positive that
# misleads operators about whether the data actually landed.
_etl_pipeline_had_uncaught_exception = False

for folder in input_data_folders:
    try:
        if  'all' in input_jobs:
            logger.info(f"Processing folder: {folder}")
            logger.info("Running all ETL jobs: format -> deduplicate -> map -> fix -> upload")

            print("""
      

                            ▄█  ░█▀▀▀ ░█       ░█▀▀█ ░█ ░█  █▀▀█ ░█▄ ░█ ░█▀▀█ ░█▀▀▀ ▀█▀ ░█▄ ░█  █▀▀█ ▀▀█▀▀ ░█▀▀▀█ ░█▀▀█
                             █  ░█▀▀▀ ░█    ▀▀ ░█    ░█▀▀█ ░█▄▄█ ░█░█░█ ░█ ▄▄ ░█▀▀▀ ░█  ░█░█░█ ░█▄▄█  ░█   ░█  ░█ ░█▄▄▀
                            ▄█▄ ░█    ░█▄▄█    ░█▄▄█ ░█ ░█ ░█ ░█ ░█  ▀█ ░█▄▄█ ░█▄▄▄ ▄█▄ ░█  ▀█ ░█ ░█  ░█   ░█▄▄▄█ ░█ ░█
  
      
            """)

            run_formatters_jobs(folder)
            run_deduplicator_jobs(folder)
            run_mappers_jobs(folder)
            run_fixers_jobs(folder)
            run_upload_jobs(folder)

        else :

            logger.info(f"Processing folder: {folder}")

            if  'format' in input_jobs:
                logger.info("Starting FORMAT job")
            if  'deduplicate' in input_jobs:
                logger.info("Starting DEDUPLICATE job")
            if  'map' in input_jobs:
                logger.info("Starting MAP job")
            if  'fix' in input_jobs:
                logger.info("Starting FIX job")
            if  'upload' in input_jobs:
                logger.info("Starting UPLOAD job")

            print("""
      

                            ▄█  ░█▀▀▀ ░█       ░█▀▀█ ░█ ░█  █▀▀█ ░█▄ ░█ ░█▀▀█ ░█▀▀▀ ▀█▀ ░█▄ ░█  █▀▀█ ▀▀█▀▀ ░█▀▀▀█ ░█▀▀█
                             █  ░█▀▀▀ ░█    ▀▀ ░█    ░█▀▀█ ░█▄▄█ ░█░█░█ ░█ ▄▄ ░█▀▀▀ ░█  ░█░█░█ ░█▄▄█  ░█   ░█  ░█ ░█▄▄▀
                            ▄█▄ ░█    ░█▄▄█    ░█▄▄█ ░█ ░█ ░█ ░█ ░█  ▀█ ░█▄▄█ ░█▄▄▄ ▄█▄ ░█  ▀█ ░█ ░█  ░█   ░█▄▄▄█ ░█ ░█
  
      
            """)

            if  'format' in input_jobs:
                run_formatters_jobs(folder)     

            if  'deduplicate' in input_jobs:
                run_deduplicator_jobs(folder)

            if  'map' in input_jobs:
                run_mappers_jobs(folder)

            if  'fix' in input_jobs:
                run_fixers_jobs(folder)

            if  'upload' in input_jobs:
                run_upload_jobs(folder)

    except Exception as _folder_exc:
        # An uncaught exception escaped one of the run_*_jobs() helpers
        # for this folder. Log full traceback and remember to fail the
        # parent process at exit -- but keep iterating remaining folders
        # so the operator gets partial-success visibility on multi-folder
        # batches. The scheduler always passes one folder per batch, so
        # in practice this fires for the only folder of the run.
        _etl_pipeline_had_uncaught_exception = True
        logger.error(
            f"Uncaught exception while processing folder {folder}: {_folder_exc}",
            exc_info=True,
        )


print("""
      

                                                    ▀▀█▀▀ ░█ ░█ ░█▀▀▀ 　 ░█▀▀▀ ░█▄ ░█ ░█▀▀▄ 　 █ 
                                                     ░█   ░█▀▀█ ░█▀▀▀ 　 ░█▀▀▀ ░█░█░█ ░█ ░█ 　 ▀ 
                                                     ░█   ░█ ░█ ░█▄▄▄ 　 ░█▄▄▄ ░█  ▀█ ░█▄▄▀ 　 ▄
            
    
      """)

# Decide the parent process exit code so the scheduler's
# `result.returncode` correctly reflects whether the batch actually
# completed end-to-end. Two distinct signals:
#   1. _etl_pipeline_had_uncaught_exception -- something escaped the
#      run_*_jobs() helpers' own try/except blocks (folder-level).
#   2. cf._check_failure_flag() -- a subprocess (formatter/fixer/mapper)
#      wrote a failure message to the queue, OR print_failure_message()
#      was invoked anywhere in the parent. This flag was already being
#      set in many places but never consulted at exit -- in one past production batch
#      for example: 4 formatters failed,
#      the dedup phase silently failed for every table, and the parent
#      still exited 0 because nothing checked the flag.
_pipeline_failed = _etl_pipeline_had_uncaught_exception
try:
    if cf._check_failure_flag():
        _pipeline_failed = True
except Exception as _flag_exc:
    logger.warning(
        f"Could not check failure flag at pipeline exit: {_flag_exc}",
        exc_info=True,
    )

if _pipeline_failed:
    logger.error(
        "ETL pipeline finished with one or more job failures. "
        "Exiting with non-zero status so the caller (scheduler or CLI) "
        "marks this batch as ERROR."
    )
    sys.exit(1)
             