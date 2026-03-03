
############### Pyspark Settings ###############

PYSPARK_WORKERS_COUNT = 5        # default to 5 workers.


############### Paths Settings ###############

CREDENTIALS_PATH             = '/path/to/credentials/'
OVID_INSTALL_PATH            = '/path/to/ovid/'
OMOP_CONCEPT_TABLES_PATH     = '/path/to/omop_vocabulary_tables/'


############### SNOWFLAKE Settings ###############

DB_TYPE        = 'sf'     # Do not change!
DB_SERVER_NAME =  "your-account.region.snowflakecomputing.com"  
SF_STAGE_NAME  =  "YOUR_STAGING_AREA"
SF_ROLE        =  "YOUR_SNOWFLAKE_ROLE"
SF_DB_USER     =  "YOUR_SNOWFLAKE_USER"

SF_WAREHOUSE   =  "YOUR_WAREHOUSE_NAME"
SF_PRIVATE_KEY_PASSPHRASE_FILE_NAME =  "your_passphrase_file.txt"
SF_PRIVATE_KEY_FILE_NAME            =  "your_private_key.p8"


############### ENCRYPTION Settings ###############

SEED_FILE_NAME = "your_encryption_seed.txt"

############### DATAVANT Settings ###############

DV_CREDENTIALS_FILE_NAME                 =  "your_datavant_credentials.txt"
DV_TRANSFORMATION_SCRIPT_FILE_NAME       =  'Datavant_Linux_ubuntu2004_x64'

