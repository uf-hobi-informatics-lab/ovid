import sys
import os

# Add project root to Python path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from common.ovidFunctions import OvidFunctions
import subprocess
from ovid_setup.install_settings import *
from common.settings import *

# print welcome to Ovid install



# make sure there is no ovid in that location


# print we what we are using and press yes if all good


        # create and load the image
        # create the secrets.py
        # copy concept_tables
        # create dockercompose file
        # create the partner
            # create partner settings

# print  SUCCUESS 

ov =OvidFunctions()
padding = '                            '
menu_padding = '            '

###################################################################################################################################
# This function will read user input
###################################################################################################################################

def print_installer_menu():

#    padding = '               '
    print('\n')
    ov.print_with_style( menu_padding+ '[1]: Install Ovid', 'italic white')
    ov.print_with_style( menu_padding+ '[2]: Create Partner', 'italic white')
    # ov.print_with_style( menu_padding+ '[3]: Import Partner', 'italic white')
    ov.print_with_style( menu_padding+ '[e]: exit','italic white')






###################################################################################################################################
# This function display an error message when a bad input is detected
###################################################################################################################################

def display_error():

    print('Invalid input !!!')

###################################################################################################################################
# This function will read user input
###################################################################################################################################

def getUserCommnad():


    userInput = input(':')

    if userInput.lower() == '1': return '1'
    elif userInput.lower() == '2': return '2'
    elif userInput.lower() == '3': return '3'
    elif userInput.lower() in ('e', 'exit'): return 'exit'
    else: return 'error'

###################################################################################################################################
# This function will verify docker is installed and running
###################################################################################################################################
def verify_docker():

    try:

        print('\n')
        ov.print_with_style( f"1/5  Verifying Docker: ", 'italic white')
        result = subprocess.run(["docker", "--version"], capture_output=True, text=True, check=True)



        ov.print_with_style( padding+"✅  Docker detected: "+ result.stdout.strip(), 'matrix green')

        return True
    
    except FileNotFoundError:
        print(padding+"❌ Docker is not installed or not in PATH")
        return False
    
    except subprocess.CalledProcessError as e:
        print(padding+"⚠️ Error running Docker command:", e)

        return False





###################################################################################################################################
# This function will verify if prerequisits are met or not
###################################################################################################################################

def verify_prerequisites():

   return verify_docker()


###################################################################################################################################
# This function will verify if prerequisits are met or not
###################################################################################################################################

def build_and_load_docker_image():

    image_name      = 'ovid_image'
    dockerfile_path = './ovid_setup/ovid_image/'
    container_name  = 'ovid_container'

    try:
        # Step 1: Build the Docker image
        # print_green(f"🔧 Building Docker image '{image_name}'...")

    #    padding = '               '
       
        print('\n')
        ov.print_with_style(f"2/5  Building Docker image '{image_name}': ", 'italic white')

        result = subprocess.run(
            ["docker", "build", "-t", image_name, dockerfile_path],
            check=True,
            capture_output=True
        )

   

        # print_green(f"✅ Successfully built image '{image_name}'.")
        ov.print_with_style( padding+ f"✅ Successfully built image '{image_name}'.", 'matrix green')

###################################################################################################################################

        print('\n')
        ov.print_with_style(f"3/5  Running container '{container_name}:", 'italic white')



        # Step 2: Remove any existing container with the same name
        subprocess.run(["docker", "rm", "-f", container_name], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

        # Step 3: Run the Docker container
        # print_green(f"🚀 Running container '{container_name}'...")


       #cmd = ["docker", "run", "--name", container_name, image_name]
        cmd =  ["docker", "run", "--rm", "--entrypoint", "bash", image_name]
        subprocess.run(cmd, check=True)

        # print_green(f"✅ Container '{container_name}' is running.")
        ov.print_with_style( padding+ f"✅ Container '{container_name}' is running", 'matrix green')
###################################################################################################################################
    except subprocess.CalledProcessError as e:

        # print_red(f"❌ Error: {e}")
        ov.print_with_style( padding+ f"❌ Error: {e}", 'danger red')

        sys.exit(1)

###################################################################################################################################
# This function will create ovid_secrets.py for snowflake and place it in the common folder
###################################################################################################################################

def create_ovid_secrets_snowflake():

#    padding = '               '

    print('\n')
    ov.print_with_style("4/5  Creating ovid_secrets.py:", 'italic white')

    try:
        lines = [
            
            "########### Ovid secrets ###############\n", 
            "\n",
            f"CREDENTIALS_PATH                    =  '{CREDENTIALS_PATH}'\n",
            "\n",
            "########### SNOWFLAKES secrets ###############\n", 
            "\n",
            f"SF_DB_USER                          =  '{SF_DB_USER}'\n",
            f"SF_ROLE                             =  '{SF_ROLE}'\n",
            f"SF_WAREHOUSE                        =  '{SF_WAREHOUSE}'\n",
            f"SF_PRIVATE_KEY_PASSPHRASE_FILE_NAME =  '{SF_PRIVATE_KEY_PASSPHRASE_FILE_NAME}'\n",
            f"SF_PRIVATE_KEY_FILE_NAME            =  '{SF_PRIVATE_KEY_FILE_NAME}'\n",
            f"SF_STAGE_NAME                       =  '{SF_STAGE_NAME}'\n",
            "\n",
            "########### Encryption secrets ###############\n", 
            "\n",
            f"SEED_FILE_NAME                      =  '{SEED_FILE_NAME}'\n",
            "\n",
            "########### Datavant secrets ###############\n", 
            "\n",
            f"DV_CREDENTIALS_FILE_NAME            =  '{DV_CREDENTIALS_FILE_NAME}'\n",

            f"DV_TRANSFORMATION_SCRIPT_FILE_NAME  =  '{DV_TRANSFORMATION_SCRIPT_FILE_NAME}'\n",
            "\n",
            ]


        with open("common/ovid_secrets.py", "w") as file:

            file.writelines(lines)
            
        ov.print_with_style( padding+ f"✅ ovid_secrets.py created", 'matrix green')

    except Exception as e:
       
         ov.print_with_style( padding+ f"❌ Error creating ovid_secrets.py: {e}", 'danger red')
                                
###################################################################################################################################
# This function will create ovid_secrets.py and place it in the common folder
###################################################################################################################################

def create_ovid_secrets():


    if DB_TYPE == 'sf':

        create_ovid_secrets_snowflake()

    

    

###################################################################################################################################
# This function will copy OMOP concept table into ovid/common/omop_cdm/
###################################################################################################################################

def copy_concept_tables():

    print('\n')
    ov.print_with_style("5/5  Copying the OMOP vocabluary tables:", 'italic white')

    data_folder_path = OMOP_CONCEPT_TABLES_PATH
    output_data_folder_path = './common/omop_cdm/'


    for vocabulry_table in OMOP_VOCABULARY_TABLES_NAMES :

        vocabulry_table_file_name = vocabulry_table+'.csv'
        input_file_name = output_file_name = vocabulry_table_file_name

        
        try:

            ov.copy_data(input_file_name,output_file_name,data_folder_path,output_data_folder_path )

            ov.print_with_style( padding+ f"✅ {vocabulry_table_file_name} table copied", 'matrix green')

        except Exception as e:
       
         ov.print_with_style( padding+ f"❌ Error copying {vocabulry_table_file_name}: {e}", 'danger red')
                                
  



###################################################################################################################################

###################################################################################################################################
# This function display an error message when a bad input is detected
###################################################################################################################################

def install_ovid():


    # 1. verify prequists

    prerequisites_are_met =   verify_prerequisites()


    if prerequisites_are_met:


        build_and_load_docker_image()

        create_ovid_secrets()

        copy_concept_tables()
    


###################################################################################################################################
# This function print availabe formatters templates 
###################################################################################################################################

def print_formatters_templates():

    print('\n')
    ov.print_with_style( menu_padding+ '[1]: OMOP 5.3 CDM PLUS', 'italic white')
    ov.print_with_style( menu_padding+ '[2]: OneFL OMOP_Like CDM', 'italic white')
    ov.print_with_style( menu_padding+ '[3]: PCORNet CDM', 'italic white')


###################################################################################################################################
# This function will import a partner template 
###################################################################################################################################

def import_a_template(template, partner_name):

    if template == 'omop_5.3':

        folder_name = 'omop_partner_plus'

    if template == '1fOmopLike':

        folder_name = 'omop_partner_1'

    if template == 'pcornet':

        folder_name = 'pcornet_partner_1'


    folder_to_copy_from = './ovid_setup/templates/'+ folder_name

    folder_to_copy_to = './partners/'+partner_name.lower()
    
    print('\n')

    ov.print_with_style(f"1/1  Creating partner: {partner_name.lower()}", 'italic white')
    
    ov.copy_folder(folder_to_copy_from, folder_to_copy_to)

    ov.print_with_style( padding+ f"✅  Partner {partner_name.lower()} created", 'matrix green')


###################################################################################################################################
# This function will create a partner from a predefined template
###################################################################################################################################

def create_partner():

    print_formatters_templates()


    userInput =  getUserCommnad()

    partner_name =  input ('Enter a name for the new partner: ')


    if userInput.lower() == 'exit':
        sys.exit()

    if userInput.lower() == '1':
        
        import_a_template('omop_5.3', partner_name)

    if userInput.lower() == '2':
    
        import_a_template('1fOmopLike', partner_name)

    if userInput.lower() == '3':
    
         import_a_template('pcornet', partner_name)

    if userInput.lower() == 'error':

        display_error()



###################################################################################################################################
print('\n')

ov.print_with_style("Welcome to Ovid install :)", 'matrix green')



while True :

    print_installer_menu()

    userInput =  getUserCommnad()


    if userInput.lower() == 'exit':
        sys.exit()

    if userInput.lower() == '1':
        
        install_ovid()

    if userInput.lower() == '2':
    
        create_partner()

    # if userInput.lower() == '3':
    
    #     import_partner()

    if userInput.lower() == 'error':

        display_error()

