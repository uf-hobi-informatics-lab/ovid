import os
import time
import queue
from concurrent.futures import ThreadPoolExecutor
from settings import *
import sys
from commonFunctions import CommonFuncitons
import datetime
import subprocess
import threading
import pytz
import snowflake.connector
print(snowflake.connector.__version__)


active_jobs = set()  # store job IDs or unique identifiers
active_jobs_lock = threading.Lock()




job_queue = queue.Queue()
cf =CommonFuncitons('')



    ###################################################################################################################################
    ###################################################################################################################################



def batch_exist_in_scheduler_db(batch_id):

    query = f"SELECT COUNT(*) FROM {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch WHERE batch_id = '{batch_id}'"

    result = cf.execute_query(query)


    count = result[0][0] if result and len(result) > 0 else None

    if count == 0:
       
       return False
    
    else:

        return True



    ###################################################################################################################################
    ###################################################################################################################################



def add_new_batch_to_db(batch_partner_name, batch_folder):



    batch_id = batch_partner_name +"_"+batch_folder
    batch_path = '/data/'+batch_partner_name+'/'+batch_folder 

    batch_received_time_float = os.path.getctime(batch_path)
    utc_dt = datetime.datetime.utcfromtimestamp(batch_received_time_float).replace(tzinfo=pytz.utc)
    eastern = pytz.timezone("US/Eastern")
    batch_received_time_est = utc_dt.astimezone(eastern)

    # batch_received_time = datetime.datetime.fromtimestamp(batch_received_time_float).strftime('%Y-%m-%d %H:%M:%S')

    query = f"""
    
        INSERT INTO {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch
        (batch_id, path,received_time,status)
        values('{batch_id}','{batch_path}','{batch_received_time_est}','NEW');
        
        """
    result = cf.execute_query(query)



    ###################################################################################################################################
    ###################################################################################################################################



def batch_ready_to_process(batch_id):

    query = f"SELECT status, path  FROM {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch WHERE batch_id = '{batch_id}'"

    result = cf.execute_query(query)


    status = result[0][0] if result and len(result) > 0 else None
    batch_path = result[0][1] if result and len(result) > 0 else None


    batch_last_modified_time_float = os.path.getctime(batch_path)
    batch_last_modified_time = datetime.datetime.fromtimestamp(batch_last_modified_time_float)


    now = datetime.datetime.utcnow()

    time_diff_in_minutes = (now - batch_last_modified_time).total_seconds() / 60

    if time_diff_in_minutes > MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES and status == 'NEW':
        return True
    else:

        return False
    ###################################################################################################################################
    ###################################################################################################################################

def job_key(job):
    """Return a unique key for a job."""
    # Adjust depending on what defines 'same job'
    return (job["ovid_partner"], job["ovid_folder"])

    ###################################################################################################################################
    ###################################################################################################################################

def run_job_with_cleanup(job):
    try:
        run_job(job)  # Your actual job function
    finally:
        # Remove from active set when done
        with active_jobs_lock:
            active_jobs.discard(job_key(job))


    ###################################################################################################################################
    ###################################################################################################################################

def fetch_new_jobs():
    """
    Traverses all partner folders and returns the first job file
    that was changed more than 1 hour ago.

    :param partners_folder: List of partner folders to check.
    :return: Path to the job file or None if no job is found.
    """
    
    data_path = '/data'


    jobs = []
    



    for partner_folder in os.listdir(data_path):
        partner_folder_path = os.path.join(data_path, partner_folder)
        if os.path.isdir(partner_folder_path) and partner_folder.lower() in partners_list:


            for data_folder in os.listdir(partner_folder_path):
                batch_id = partner_folder+'_'+data_folder

                if batch_exist_in_scheduler_db(batch_id):



                    if  batch_ready_to_process(batch_id):


                        target_db = 'char_' + partner_folder

                        job_info = {
                            "ovid_folder": f"{data_folder}",
                            "ovid_tables": "all",
                            "ovid_jobs": "all",
                            "ovid_partner": f"{partner_folder.lower()}",
                            "ovid_target_db": f"char_{partner_folder.lower()}",
                            "ovid_db_type": SCHEDULER_DB_TYPE,
                            "ovid_db_server": SCHEDULER_DB_SERVER,
                            "ovid_db": target_db,
                            "ovid_input_data_folder_name": f"/data/{partner_folder.upper()}/"
                        }

                        jobs.append(job_info)

                    else:

                        continue
                        
                else:




                    add_new_batch_to_db(partner_folder, data_folder)

                    

            
    # print('########################################################################')
    # print('#################### ALL JOBS    #######################################')
    # print(jobs)
    return jobs               

    ###################################################################################################################################
    ###################################################################################################################################

def create_ovid_db_if_not_found():

    # print("create_ovid_db_if_not_found")

    # print(cf.check_scheduler_db_exists())
    if not cf.check_scheduler_db_exists():
        # print('not found')
        cf.create_scheduler_db()


    # print("end create_ovid_db_if_not_found")

    ###################################################################################################################################
    ###################################################################################################################################

def update_batch_field(ovid_partner, ovid_folder,field_name, value):


    batch_id = ovid_partner +'_'+ovid_folder


    query = f"""

                UPDATE {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.DATA_BATCH
                SET {field_name} = '{value}'
                WHERE BATCH_ID ILIKE '{batch_id}' 
               
               ;
        
            """
    
    result = cf.execute_query(query)




    ###################################################################################################################################
    ###################################################################################################################################

def run_job(job):

    current_time_str =  cf.get_current_time()

        
    update_batch_field(job["ovid_partner"].upper(), job["ovid_folder"].upper(),'status', 'PROCESSING')
    update_batch_field(job["ovid_partner"].upper(), job["ovid_folder"].upper(),'processing_start', current_time_str)


    try:
        # Get the Spark master URL from environment
        master_url = os.environ.get("SPARK_URL", "spark://master:7077").strip().replace('"', '')
        
        # Build spark-submit command similar to ovid.py ETL
        command = f"""
            cd /app && \
            zip -j common.zip common/*.py && \
            /opt/bitnami/spark/bin/spark-submit \
                --conf spark.local.dir=/docker_data/spark_tmp \
                --conf spark.driver.memoryOverhead=2g \
                --conf spark.executor.memoryOverhead=4g \
                --conf spark.driver.memory=32g \
                --conf spark.sql.adaptive.enabled=true \
                --master {master_url} \
                --py-files common.zip \
                /app/pipelines/etl.py \
                -p {job['ovid_partner']} \
                -j {job['ovid_jobs']} \
                -t {job['ovid_tables']} \
                -f {job['ovid_partner'].upper()}/{job['ovid_folder']} \
                -dt {job['ovid_db_type']} \
                -s {job['ovid_db_server']} \
                -db {job['ovid_db']} ; \
            rm -f common.zip
        """
        
        # Execute the command
        result = subprocess.run(command, shell=True)

        current_time_str = cf.get_current_time()

        if result.returncode == 0:
            update_batch_field(job["ovid_partner"].upper(), job["ovid_folder"].upper(),'status', 'PROCESSED')
        else:
            update_batch_field(job["ovid_partner"].upper(), job["ovid_folder"].upper(),'status', 'ERROR')
        update_batch_field(job["ovid_partner"].upper(), job["ovid_folder"].upper(),'processing_end', current_time_str)


    except Exception as e:
        current_time_str = cf.get_current_time()

                   
        update_batch_field(job["ovid_partner"].upper(), job["ovid_folder"].upper(),'status', 'ERROR')
        update_batch_field(job["ovid_partner"].upper(), job["ovid_folder"].upper(),'processing_end', current_time_str)


        cf.print_failure_message(
                                folder  = job['ovid_partner'] ,
                                partner = job['ovid_partner'].lower(),
                                job     = job ,
                                text = str(e)
                                )


    ###################################################################################################################################
    ###################################################################################################################################

def scheduler():

    create_ovid_db_if_not_found()

    with ThreadPoolExecutor(max_workers=SCHEDULER_MAX_THREADS) as executor:

            new_jobs = fetch_new_jobs()

            for job in new_jobs:
                key = job_key(job)
                with active_jobs_lock:
                    if key not in active_jobs:
                        job_queue.put(job)
                        active_jobs.add(key)

  
            if not job_queue.empty():
                job = job_queue.get()
                executor.submit(run_job_with_cleanup, job)



    ###################################################################################################################################
    ###################################################################################################################################

if __name__ == "__main__":

    print(f"Starting OVID Scheduler (polling every {SCHEDULER_POLL_INTERVAL_IN_MINUTES} minutes)...")
    
    while True:
        try:
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Checking for new jobs...")
            scheduler()
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Sleeping for {SCHEDULER_POLL_INTERVAL_IN_MINUTES} minutes...")
            time.sleep(SCHEDULER_POLL_INTERVAL)
        except KeyboardInterrupt:
            print("\nScheduler stopped by user.")
            break
        except Exception as e:
            print(f"[{datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Error in scheduler: {e}")
            time.sleep(SCHEDULER_POLL_INTERVAL)
