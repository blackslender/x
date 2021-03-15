from update_and_upsert import DataLoaderUpdate, DataLoaderUpsert
from overwrite import DataLoaderOverwrite
from append import DataLoaderAppend
from pyspark.sql import functions as F, types as T
import pyspark
from functools import reduce
import yaml


class DataLoader:

    _staging_table_name = "STAGING_TABLE"

    @staticmethod
    def init_dataloader(config_yaml_filepath, params={}):
        with open("dummy.yaml", "r") as f:
            raw_config = f.read()
            for key in params:
                raw_config = raw_config.replace(key, params[key])
            config = yaml.safe_load(raw_config)

        # All config key should be lowercase
        for key in list(config.keys()):
            config[key.lower()] = config[key]

        # Config validation: TODO
        # From "source", there should be only one subkey in "table" or "query".
        assert "source" in config
        assert len(config["source"]) == 1
        # If the source data is in table, convert it to query
        if "table" in config["source"]:
            config["source"]["query"] = "SELECT * FROM {}".format(
                config["source"]["table"])
        assert "query" in config["source"]

        assert "target" in config
        # From "target", these are required:

        # - "table"
        assert "table" in config["target"]
        # - "operation"
        assert "operation" in config["target"]
        # TODO: Make other validations here

        operation = config["target"]["operation"]
        if operation.lower() == "overwrite":
            return DataLoaderOverwrite(config, params=params)
        if operation.lower() in ["append", "insert"]:
            return DataLoaderAppend(config, params=params)
        elif operation.lower() == "update":
            return DataLoaderUpdate(config, params=params)
        elif operation.lower() == "upsert":
            return DataLoaderUpsert(config, params=params)

    def __init__(self, config, params):
        """
        DO NOT USE CONSTRUCTOR TO CREATE DATALOADER OBJECT. Instead, use static 'init_dataloader' as an object factory.
        When overwriting this constructor, the parent constructor should be called as super(DataLoaderChildClass, self).__init__(config, params)

        Parameters:
        - config: Dictionary of job config
        - params: optional, the dynamic parameters as a dict of (param - value).
        """
        if type(self) is DataLoader:
            raise Exception("DataLoader class is abstract.")

        self.config = config
        self.version = self.config["version"]
        self.params = params

    def __repr__(self):
        return str(self.config)

    # Script generator

    def generate_pre_script(self):
        """Generate and return pre-script"""
        # Pre-script includes:
        # - Truncate table if needed
        # - "Delete where" clause if needed
        # - Create staging table if needed
        # - User's pre-script if provided
        # If no pre-script is required, this should return "select 1" as a dummy query

        # Default behaviour: return dummy script
        pre_sql = ""

        if "pre_sql" in self.config["target"]:
            return self.config["target"]["pre_sql"]
        else:
            return "SELECT 1 as c1"
        return pre_sql
    # TODO: Tach 4 phuong thuc ra file khac

    def generate_main_script(self):
        """Generate and return main script"""
        # This method is abstract and required to be re-implemented

        raise NotImplementedError

    def generate_post_script(self):
        """Generate and return post script"""

        # Default behaviour: return dummy script
        return "SELECT 1 as c1"

    def generate_job_full_script(self):
        """Generate the whole job's script"""

        return ";\n".join([self.generate_pre_script(), self.generate_main_script(), self.generate_post_script()])

    # Script executor

    def execute_script(self, script):
        """Execute a script"""
        # It is the user that is responsible to validate the script
        return spark.sql(script)

    def create_staging_table(self):
        """Fetch the source data and store into a table called 'pyzzle_staging_table'"""

        (self.execute_script(self.config["query"])
         .write
         .format("delta")
         .mode("overwrite")
         .saveAsTable(self._staging_table_name))
        raise self._staging_table_name

    def drop_staging_table(self):
        """Drop 'pyzzle_staging_table' table"""
        return self.execute_script("DROP TABLE " + self._staging_table_name)

    def execute_pre_script(self):
        """Generate and execute the pre-script"""
        return self.execute_script(self.generate_pre_script())

    def execute_main_script(self):
        """Generate and execute the main script"""
        return self.execute_script(self.generate_main_script())

    def execute_post_script(self):
        """Generate and execute the main script"""
        return self.execute_script(self.generate_post_script())

    def run(self):
        """Execute the job"""

        # Create staging table if needed
        if "create_staging_table" not in self.config["target"]:
            self.config["target"]["create_staging_table"] = False
        if self.config["target"]["create_staging_table"]:
            self.create_staging_table()

        self.execute_pre_script()
        self.execute_main_script()
        self.execute_post_script()

        if self.config["target"]["create_staging_table"]:
            self.drop_staging_table()
