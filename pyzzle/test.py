import yaml

config_yaml_filepath = './testcases/job_config/test_case_multi_line.yml'
config_yaml_filepath = './testcases/job_config/test_case_overwrite.yml'

with open(config_yaml_filepath, "r") as f:
    raw_config = f.read()
    config = yaml.safe_load(raw_config)

config

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
