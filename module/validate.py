
############### test

# version: 0
# source:
#   table: slide.f_booking_stg
# target:
#   table: adw.f_booking 
#   operation: append
#   create_temp_table: False
#   where_statement_on_table: False
#   primary_key_column: False
#   update_column: False
#   pre_sql: 



job_config = {'version': 1, 'source': {'table': 't3'}, 'target': {'table': 't1', 'operation': 'update', 'where_statement_on_table': 'tgt.c1 is not null', 'primary_key_column': ['pk1', 'pk2'], 'update_column' : ['c1', 'c2', 'c3'] }}

job_config = {'version': 1, 'source': {'table': 't3'}, 'target': {'table': 't1', 'operation': 'appendx' }}

validate_job_config(job_config)

#####################

# check missing mandantory item: source table/source query, target table, operation
## check if target table exist

def pass_check_mandatory_param(job_config):
    if "table" not in job_config["source"] and "query" not in job_config["source"]:
        print("missing source table or query")
        return False
    if "table" not in job_config["target"]:
        print("missing target table")
        return False
    if "operation" not in job_config["target"]:
        print("missing operation")        
        return False        
    if job_config["target"]["operation"] not in ["append", "overwrite", "update", "upsert"]:
        print("wrong operation")        
        return False
    return True

def pass_check_target_table_exists(job_config):
    sql_string = ''' show create table {table_name} ; '''.format(table_name = job_config["target"]["table"])
    # wait for execute function from parent class
    if execute(sql_string) != None:
        return True
    else:
        return False

## overwrite: none
## insert: none

## update & upsert:  
### check missing mandantory item: primary_key_column, update_column
def pass_check_mandatory_param_update_and_upsert(job_config):
    if "primary_key_column" not in job_config["target"]:
        print("missing primary_key_column")
        return False
    if "update_column" not in job_config["target"]:
        print("missing update_column")        
        return False
    return True

#### check if column in primary_key_column exist in target table
#### check if column in update_column exist in target table
### get column list of source table -> wait for parent class 
def pass_check_columm_exist(column_list_job_config, column_list_actual_table):
    if set(column_list_job_config).issubset(column_list_actual_table):
        return True
    else:
        return False

### all check
def validate_job_config(job_config):
    if pass_check_mandatory_param(job_config) and pass_check_target_table_exists:
        if job_config["target"]["operation"] in ["update", "upsert"]:
            if pass_check_mandatory_param_update_and_upsert(job_config):
            # check column exist -> pending
                return True
            else:
                return False
        return True
    else:
        return False
        


