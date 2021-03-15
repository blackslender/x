##################### test
job_config = {'version': 1, 'source': {'table': 't3'}, 'target': {'table': 't1', 'operation': 'update', 'where_statement_on_table': 'tgt.c1 is not null', 'primary_key_columns': ['pk1', 'pk2'], 'update_columns' : ['c1', 'c2', 'c3'] }}

temp_table = None
temp_table = 'x.None'

append(job_config, temp_table)
#####################

def append(job_config, temp_table):
    main_sql = '''insert into {target_table} select * from ({source_table})'''
    if temp_table != None:
        source_table = temp_table
    elif "table" in job_config["source"]:
        source_table = job_config['source']['table']
    else:
        source_table = job_config['source']['query']
    main_sql = main_sql.format(target_table = job_config['target']['table'], source_table = source_table)
    print(main_sql)
    return main_sql

