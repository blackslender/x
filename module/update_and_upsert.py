
############### test
job_config = {'version': 1, 'source': {'table': 't3'}, 'target': {'table': 't1', 'operation': 'update', 'where_statement_on_table': 'tgt.c1 is not null', 'primary_key_columns': ['pk1', 'pk2'], 'update_columns' : ['c1', 'c2', 'c3'] }}
job_config = {'version': 1, 'source': {'query': 'select * from t3'}, 'target': {'table': 't1', 'operation': 'update', 'where_statement_on_table': 'tgt.c1 is not null', 'primary_key_columns': ['pk1', 'pk2'], 'update_columns' : ['c1', 'c2', 'c3'] }}
temp_table = None
temp_table = 'x.None'
list_of_column_in_target_table = ["c1", "c2", "c3"]

test = upsert(job_config, temp_table)
test = update(job_config, temp_table)
test

#####################

# list_of_column_in_target_table = list_of_column_in_table(job_config["target"]["table"])

def generate_sql_condition_string(list_of_column, link_char_parameter):
    if len(list_of_column) == 1:
        sql_string = ''' tgt.{column} = src.{column} '''.format(column = list_of_column[0])
    else:
        sql_string = ''' tgt.{column} = src.{column} '''.format(column = list_of_column[0])
        for i in range(len(list_of_column) - 1):
            sql_string = sql_string + ''' {link_char} tgt.{column} = src.{column} '''.format(link_char = link_char_parameter ,column = list_of_column[i+1])
    return sql_string

def generate_column_list_string(list_of_column, prefix):
    if len(list_of_column) == 1:
        sql_string = ''' {pf}.{column} '''.format(column = list_of_column[0], pf = prefix)
    else:
        sql_string = ''' {pf}.{column} '''.format(column = list_of_column[0], pf = prefix)
        for i in range(len(list_of_column) - 1):
            sql_string = sql_string + ''' , {pf}.{column}'''.format(column = list_of_column[i+1], pf = prefix)
    return sql_string

def update(job_config, temp_table):
    part1_sql = '''merge into {target_table} as tgt using (select * from ({source_table})) as src on '''
    part2_sql = job_config["target"]["where_statement_on_table"] + ' and '
    part3_sql = generate_sql_condition_string(job_config['target']['primary_key_columns'], "and")
    part4_sql = ''' when matched then update set '''
    part5_sql = generate_sql_condition_string(job_config['target']['update_columns'], ",")

    if temp_table != None:
        source_table = temp_table
    elif "table" in job_config["source"]:
        source_table = job_config['source']['table']
    else:
        source_table = job_config['source']['query']

    part1_sql = part1_sql.format(target_table = job_config['target']['table'], source_table = source_table)
    update_sql_string = part1_sql + part2_sql + part3_sql + part4_sql + part5_sql
    # print(update_sql_string)
    return update_sql_string

def upsert(job_config, temp_table):
    update_sql_string = update(job_config, temp_table)
    insert_sql_string = ''' when not matched then insert ({str1}) values ({str2}) '''.format(str1 = generate_column_list_string(list_of_column_in_target_table, "tgt"), str2 = generate_column_list_string(list_of_column_in_target_table, "src"))
    merge_sql_string = update_sql_string + insert_sql_string
    # print(merge_sql_string)
    return merge_sql_string
