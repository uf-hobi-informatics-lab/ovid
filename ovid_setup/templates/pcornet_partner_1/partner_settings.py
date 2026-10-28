mappers_type = 'pcornet_mappers'

DEFAULT_PARTNER_DB_NAME =  'changeMe'

########### HARVEST ################

NETWORKID         = ''
NETWORK_NAME      = ''
DATAMARTID        = ''
DATAMART_NAME     = ''
DATAMART_PLATFORM = ''
CDM_VERSION       = '070'          # PCORnet CDM v7.0 (target of mapping_scripts/pcornet_mappers)
DATAMART_CLAIMS   = ''
DATAMART_EHR      = ''
MGMT_FIELDS_VALUE = ''

########### DATAVANT ################

DATAVANT_KEY = ''

########### Merge ################

DEFAULT_UPLOAD_TYPE = 'no_merge'  # Options "merge" and "no_merge"
MERGE_DATABASE_NAME = ''
MERGE_SCHEMA_NAME= ''

# Set True only when Ovid BUILDS the address-history tables for this partner
# (partner does not submit a native LDS_ADDRESS_HISTORY / PRIVATE_ADDRESS_HISTORY
# table) AND the partner submits incrementally (DEFAULT_UPLOAD_TYPE == 'merge').
# When True the merge upload performs zip-transition reconciliation (close the
# old ADDRESS_PERIOD_END with the new ADDRESS_PERIOD_START on ADDRESS_ZIP5 change,
# keep the incoming new-zip row). Leave False for full-refresh partners or
# partners who submit the address history already populated.
OVID_BUILDS_LDS_ADDRESS_HISTORY = False
