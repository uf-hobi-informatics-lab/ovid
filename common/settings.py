POOL_SIZE      = 10
MAX_OVERFLOW   = 5
POOL_RECYCLE   = 3600
BCP_DRIVER     = 'mssql'


DEFAULT_WORKERS_COUNT = 25

DEFAULT_MASTER_MEMORY = '8g'
DEFAULT_WORKER_MEMORY = '16g'

DEFAULT_INPUT_DIRECTORY = ''
DEFAULT_JOBS            = 'all'
DEFAULT_TABLES          = 'all'
DEFAULT_DB_TYPE         = 'sf'
DEFAULT_DB_SERVER       = ''
###################################################################################################################################
###################################################################################################################################

AWK_PASSBY_TRESHHOLD = 40000000 # Tables more than 40M rows will not run on awk deduplicte

###################################################################################################################################
###################################################################################################################################
VALID_ETL_JOBS = [  
                    'all',
                    'map',
                    'format',
                    'deduplicate',
                    'upload', 
                    'fix',
                    ]

VALID_ANALYSIS_JOBS = [  
                    'all',
                    'format',
                     'monthly_counts',
                    'distinct_values',
                   ]


VALID_DQ_CHECKS_JOBS = [  
                    'all',
                    'format',
                    'mapping_gap',
                    'mapping_report',
                    'dc_checks',
                   ]



###################################################################################################################################
###################################################################################################################################

harmonized_filed_sizes_dict = {

                            "PATID"               :"64",
                            "PATID_1"             :"64",
                            "PATID_2"             :"64",
                            "PERSON_ID"           :"64",
                            "ENCOUNTERID"         :"128",
                            "PROVIDERID"          :"64",
                            "VX_PROVIDERID"       :"64",
                            "MEDADMIN_PROVIDERID" :"64",
                            "OBSGEN_PROVIDERID"   :"64",
                            "OBSCLIN_PROVIDERID"  :"64",
                            "RX_PROVIDERID"       :"64",
                            "PROCEDURESID"        :"128",
                            "PRESCRIBINGID"       :"128",
                            
                            }

###################################################################################################################################
########################    Scheduler settings ####################################################################################
###################################################################################################################################

SCHEDULER_MAX_THREADS= 1
SCHEDULER_POLL_INTERVAL_IN_MINUTES = 1
SCHEDULER_POLL_INTERVAL = SCHEDULER_POLL_INTERVAL_IN_MINUTES * 60
SCHEDULER_DB_TYPE   = 'sf'
SCHEDULER_DB_SERVER = ''
SCHEDULER_DB_NAME   = 'OVID'
SCHEDULER_DB_SCHEMA    = 'SCHEDULER'

MINIMUM_WAIT_TIME_BEFORE_PROCESSEING_IN_MINUTES = 1  #  minutes

CREATE_BATCH_TABLE_DLL = f"""

                    CREATE TABLE {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch (
                                batch_id VARCHAR(50) PRIMARY KEY,
                                path TEXT NOT NULL,
                                received_time TIMESTAMP,
                                status VARCHAR(20) DEFAULT 'NEW',
                                priority VARCHAR(10),
                                processing_start TIMESTAMP,
                                processing_end TIMESTAMP
                    );
                    """


CREATE_BATCH_TABLE_FILE_DLL = f"""

                    CREATE TABLE {SCHEDULER_DB_NAME}.{SCHEDULER_DB_SCHEMA}.data_batch_file (
                                id BIGINT AUTOINCREMENT PRIMARY KEY,
                                batch_id VARCHAR(50) REFERENCES data_batch(batch_id) ON DELETE CASCADE,
                                file_name VARCHAR(255) NOT NULL,
                                file_size BIGINT,
                                file_hash VARCHAR(64)
                                );
                    """




###################################################################################################################################
########################    OMOP_VOCABULARY TABLES NAMES ####################################################################################
###################################################################################################################################



OMOP_VOCABULARY_TABLES_NAMES = [

                                'CONCEPT',
                                'CONCEPT_RELATIONSHIP',
                                'CONCEPT_ANCESTOR',
                                'CONCEPT_CLASS',
                                'CONCEPT_SYNONYM',
                                'DOMAIN',
                                'DRUG_STRENGTH',
                                'RELATIONSHIP',
                                'VOCABULARY',
                                'SOURCE_TO_CONCEPT_MAP',
                                ]




###################################################################################################################################
########################  PCORNET TABLES PRIMARY KEYS         ####################################################################################
###################################################################################################################################


PCORNET_PRIMARY_KEYS = {


                "DEMOGRAPHIC"        :["PATID"],
                "ENROLLMENT"         :["PATID","ENR_START_DATE","ENR_BASIS"],
                "ENCOUNTER"          :["ENCOUNTERID"],
                "DIAGNOSIS"          :["DIAGNOSISID"],
                "PROCEDURES"         :["PROCEDURESID"],
                "VITAL"              :["VITALID"],
                "DISPENSING"         :["DISPENSINGID"],
                "LAB_RESULT_CM"      :["LAB_RESULT_CM_ID"],
                "CONDITION"          :["CONDITIONID"],
                "PRO_CM"             :["PRO_CM_ID"],
                "PRESCRIBING"        :["PRESCRIBINGID"],
                "PCORNET_TRIAL"      :["PATID","TRIALID","PARTICIPANTID"],
                "DEATH"              :["PATID","DEATH_SOURCE"],
                "DEATH_CAUSE"        :["PATID","DEATH_CAUSE","DEATH_CAUSE_CODE","DEATH_CAUSE_TYPE","DEATH_CAUSE_SOURCE"],
                "MED_ADMIN"          :["MEDADMINID"],
                "PROVIDER"           :["PROVIDERID"],
                "OBS_CLIN"           :["OBSCLINID"],
                "OBS_GEN"            :["OBSGENID"],
                "HASH_TOKEN"         :["PATID","TOKEN_ENCRYPTION_KEY"],
                "LDS_ADDRESS_HISTORY":["ADDRESSID"],
                "IMMUNIZATION"       :["IMMUNIZATIONID"],
                "PAT_RELATIONSHIP"   :["PATID_1",'PATID_2','RELATIONSHIP_TYPE'],
                "EXTERNAL_MEDS"      :["EXTMEDID"],


}