# from .update_and_upsert import DataLoaderUpdate, DataLoaderUpsert
# from .overwrite import DataLoaderOverwrite
# from .append import DataLoaderAppend
import pyzzle
# from pyspark.sql import functions as F, types as T
# import pyspark
from functools import reduce
import yaml
import re


class DataLoader:

    _staging_table_name = "STAGING_TABLE"

    @staticmethod
    def init_dataloader(config_yaml_filepath, spark=None, params={}):
        if spark is None:
            raise Exception(
                "Please provide spark instance (spark=spark in Databricks)")
        with open(config_yaml_filepath, "r") as f:
            raw_config = f.read()
            for key in params:
                raw_config = raw_config.replace(f"${{{key}}}", params[key])
            config = yaml.safe_load(raw_config)

        # Make sure that all parameters are provided
        def get_required_params(text):
            param_ex = r"\$\{[A-Za-z_]+[A-Za-z0-9_]*\}"
            all_params = list(
                map(lambda x: x[2:-1], re.findall(param_ex, text)))
            return all_params

        if len(get_required_params(config_yaml_filepath)) > 0:
            raise Exception("All parameters should be provided. Please provide " +
                            str(get_required_params(config_yaml_filepath)))

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
            return pyzzle.DataLoaderOverwrite(config, spark=spark, params=params)
        if operation.lower() in ["append", "insert"]:
            return pyzzle.DataLoaderAppend(config, spark=spark, params=params)
        elif operation.lower() == "update":
            return pyzzle.DataLoaderUpdate(config, spark=spark, params=params)
        elif operation.lower() == "upsert":
            return pyzzle.DataLoaderUpsert(config, spark=spark, params=params)

    def __init__(self, config, spark=None, params={}):
        r"""
        DO NOT USE CONSTRUCTOR TO CREATE DATALOADER OBJECT. Instead, use static 'init_dataloader' as an object factory.
        When overwriting this constructor, the parent constructor should be called as super(DataLoaderChildClass, self).\_\_init\_\_(config, spark, params)

        Parameters:
        - config: Dictionary of job config
        - params: optional, the dynamic parameters as a dict of (param - value).
        """
        if type(self) is DataLoader:
            raise Exception("DataLoader class is abstract.")

        self.config = config
        self.version = self.config["version"]
        self.spark = spark
        self.params = params

        # If no create_staging_table option is provided, default is False (no creating)
        if "create_staging_table" not in self.config["target"]:
            self.config["target"]["create_staging_table"] = False

        pyzzle.JobConfigValidator(
            self, print_log=False).validate_all(raise_exception=True)

    def validate(self):
        return pyzzle.JobConfigValidator(self, print_log=True).validate_all()

    def __repr__(self):
        return str(self.config)

    # Script generator

    def generate_pre_script(self):
        """Generate and return pre-script"""
        pre_sql = ""

        if "pre_sql" in self.config["target"]:
            pre_sql += self.config["target"]["pre_sql"]
        else:
            pre_sql += "SELECT 1 as c1"

        if self.config["target"]["create_staging_table"]:
            pre_sql += """
DROP TABLE IF EXISTS {staging_table};
CREATE TABLE {staging_table} USING DELTA AS
SELECT * FROM (\n{source_query}n)
            """
        return pre_sql

    def generate_main_script(self):
        """Generate and return main script"""
        # This method is abstract and required to be re-implemented

        raise NotImplementedError

    def generate_post_script(self):
        """Generate and return post script"""

        post_sql = ""

        if "post_sql" in self.config["target"]:
            post_sql += self.config["target"]["post_sql"]
        else:
            post_sql += "SELECT 1 as c1"

        if self.config["target"]["create_staging_table"]:
            post_sql += """
DROP TABLE IF EXISTS {staging_table};
            """
        return post_sql

    def generate_job_full_script(self):
        """Generate the whole job's script"""
        return ";\n".join([self.generate_pre_script(), self.generate_main_script(), self.generate_post_script()])

    # Script executor
    def execute_script(self, script):
        """Execute a script"""
        # It is the user that is responsible to validate the script
        return self.spark.sql(script)

    def create_staging_table(self):
        """Fetch the source data and store into a table called 'pyzzle_staging_table'"""

        return self.execute_script(self.config["source"]["query"])\
            .write \
            .format("delta") \
            .mode("overwrite") \
            .saveAsTable(self._staging_table_name)

    def drop_staging_table(self):
        """Drop staging table"""
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
        self.execute_pre_script()
        self.execute_main_script()
        self.execute_post_script()
