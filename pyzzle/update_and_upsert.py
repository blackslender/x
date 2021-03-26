from pyzzle import DataLoader


def generate_sql_condition_string(list_of_column, link_char_parameter):
    return (" " + link_char_parameter + " ").join(map(lambda x: "TGT.{} = SRC.{}".format(x, x), list_of_column))


def generate_column_list_string(list_of_column, prefix):
    return ", ".join(map(lambda x: "{}.{}".format(prefix, x), list_of_column))


def update(job_config):

    if "table" in job_config["target"]:
        target_table = job_config["target"]["table"]
    else:
        target_table = "delta.`{}`".format(job_config["target"]["path"])

    if "where_statement_on_table" not in job_config["target"]:
        job_config["target"]["where_statement_on_table"] = "1=1"
    part0_sql = '''CREATE OR REPLACE TEMPORARY VIEW __target_view AS TABLE {};\n'''.format(
        target_table)
    part1_sql = '''MERGE INTO __target_view AS TGT \nUSING __source_view AS SRC \nON '''
    part2_sql = job_config["target"]["where_statement_on_table"] + ' AND '
    part3_sql = generate_sql_condition_string(
        job_config['target']['primary_key_column'], "AND")
    part4_sql = '''\nWHEN MATCHED THEN \n\tUPDATE SET '''
    part5_sql = generate_sql_condition_string(
        job_config['target']['update_column'], ",")

    update_sql_string = part1_sql + part2_sql + part3_sql + part4_sql + part5_sql
    # print(update_sql_string)
    return update_sql_string


def upsert(job_config):
    update_sql_string = update(job_config)

    insert_sql_string = '''\nWHEN NOT MATCHED THEN \n\tINSERT ({str1}) VALUES ({str2}) '''.format(str1=generate_column_list_string(
        job_config["target"]["update_column"], "TGT"), str2=generate_column_list_string(job_config["target"]["update_column"], "SRC"))
    merge_sql_string = update_sql_string + insert_sql_string
    # print(merge_sql_string)
    return merge_sql_string


class DataLoaderUpdate(DataLoader):

    def __init__(self, config, spark=None,  params={}):
        super(DataLoaderUpdate, self).__init__(
            config, spark=spark, params=params)
        # # TODO
        assert self.config["target"]["operation"] == "update"
        assert "primary_key_column" in self.config["target"]
        assert "update_column" in self.config["target"]

    def step_06_operate(self, generate_sql=False):
        if "table" in self.config["target"]:
            target_table = self.config["target"]["table"]
        elif "path" in self.config["target"]:
            target_table = "delta.`{}`".format(target_table)

        script = update(self.config)
        if generate_sql:
            return script
        else:
            return self.execute_script(script)


class DataLoaderUpsert(DataLoader):

    def __init__(self, config, spark=None, params={}):
        super(DataLoaderUpsert, self).__init__(
            config, spark=spark, params=params)
        # # TODO
        assert self.config["target"]["operation"] == "upsert"
        assert "primary_key_column" in self.config["target"]
        assert "update_column" in self.config["target"]

    def step_06_operate(self, generate_sql=False):
        if "table" in self.config["target"]:
            target_table = self.config["target"]["table"]
        elif "path" in self.config["target"]:
            target_table = "delta.`{}`".format(target_table)

        script = upsert(self.config)
        if generate_sql:
            return script
        else:
            return self.execute_script(script)
