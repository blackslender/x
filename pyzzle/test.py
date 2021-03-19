import yaml

config_yaml_filepath = './testcases/job_config/test_case_multi_line.yml'
config_yaml_filepath = './testcases/job_config/test_case_overwrite.yml'
config_yaml_filepath = './testcases/job_config/test_case_overwrite_all_data.yml'

with open(config_yaml_filepath, "r") as f:
    raw_config = f.read()
    config = yaml.safe_load(raw_config)

            partition_columns = config["target"]["partition_column"]

print(partition_columns== None)

print(config["target"]["partition_column"] == "")

condition_string = " OR ".join( [] )
condition_string

print(config["source"]["query"])


    def generate_support_script(config, support_type):
        """Generate and return pre-script"""
        # Pre-script includes:
        # - Truncate table if needed
        # - "Delete where" clause if needed
        # - Create staging table if needed
        # - User's pre-script if provided
        # If no pre-script is required, this should return "select 1" as a dummy query

        # Default behaviour: return dummy script
        system_type = support_type["system_type"]
        script_type = support_type["script_type"]
        pre_sql = ""

        if script_type in config.get(system_type):
            return " ; ".join(config.get(system_type).get(script_type))
        else:
            return "SELECT 1 as c1"
        return support_sql


x = generate_support_script(config, { "system_type":"source", "script_type":"pre_sql" })
#######

    def _generate_key_matching_condition_string(self):
        if self.config["target"]["create_staging_table"]:
            source_table = self.spark.table(self._staging_table_name)
        else:
            source_table = self.execute_script(self.config["source"]["query"])

        if self.config["target"]["partition_column"]:
            partition_columns = self.config["target"]["partition_column"]
        else:
            partition_columns = self._get_target_table_partition_columns(self.config["target"]["table"])

        distinct_partition_values = [{"pk2":[1,2]}, {"c2":[1,0,2]}, {"c3":[4,1,2]}]
        partition_columns = None

        distinct_partition_values = list(map(lambda x: x.asDict(
        ), source_table.select(*partition_columns).distinct().collect()))        

        condition_string = " OR ".join(map(lambda row: "(" + " AND ".join(map(
            lambda key: "{key} = '{value}'".format(key=key, value=row[key]), row)) + ")", distinct_partition_values))

        condition_string

        if condition_string == "":
            condition_string = "1=1"
        return condition_string