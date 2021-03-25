import pyzzle
from functools import reduce
import yaml
import re


class DataLoader:

    @staticmethod
    def init_dataloader(config_yaml_filepath, spark=None, params={}):
        if spark is None:
            raise Exception(
                "Please provide spark instance (provide spark=spark in Databricks)")

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

        try:
            operation = config["target"]["operation"]
        except KeyError as e:
            raise KeyError("The target - operation key is required for a job.")

        if operation.lower() == "overwrite":
            return pyzzle.DataLoaderOverwrite(config, spark=spark, params=params)
        if operation.lower() in ["append", "insert"]:
            return pyzzle.DataLoaderAppend(config, spark=spark, params=params)
        elif operation.lower() == "update":
            return pyzzle.DataLoaderUpdate(config, spark=spark, params=params)
        elif operation.lower() == "upsert":
            return pyzzle.DataLoaderUpsert(config, spark=spark, params=params)
        else:
            raise ValueError("Unexpected oparation '%s'" % operation)

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

        # TODO: Complete the validation module
        # For now, it's temporarily disabled
        # pyzzle.JobConfigValidator(
        #     self, print_log=False).validate_all(raise_exception=True)

    def validate(self):
        return pyzzle.JobConfigValidator(self, print_log=True).validate_all()

    def __repr__(self):
        return str(self.config)

    # Script executor

    def execute_script(self, script):
        """Execute a script"""

        # Users are responsible to validate the script
        script = script.replace("\n", " ")
        if script.replace(" ", "") == "":
            return None
        if ";" not in script:
            return self.spark.sql(script)
        else:
            # If a multi-statement script is provided, return the result of last statement.
            statements = filter(
                lambda x: x != "", map(
                    lambda x: x.strip(), script.split(";")))

            return list(map(lambda x: self.spark.sql(x), statements))[-1]

    def step_01_source_pre_sql(self, generate_sql=False):
        if "pre_sql" not in self.config["source"]:
            script = ""
        else:
            script = self.config["source"]["pre_sql"]

        if generate_sql:
            return script
        else:
            return self.execute_script(script)

    def step_02_create_reference_views(self, generate_sql=False):
        """ Create temp views related to reference table paths from source config
        Parameters:
            + paths: dictionary of (view_name, table path)
            + generate_sql: bool. If False: execute creating the view, else return the sql script only (no execution).
        Return: dictionary of (view_name, dataframe) if generate_sql=True else str - the script"""

        def create_view_ddl(args):
            view_name, path = args
            return "CREATE OR REPLACE TEMPORARY VIEW {} AS SELECT * FROM delta.`{}`;".format(view_name, path)

        if "reference_table_path" not in self.config["source"]:
            script = ""
        else:
            if isinstance(self.config["source"]["reference_table_path"], list):
                r = dict()
                for subdict in self.config["source"]["reference_table_path"]:
                    r = {**r, **subdict}
                self.config["source"]["reference_table_path"] = r

            script = ";\n".join(
                map(
                    create_view_ddl,
                    self.config["source"]["reference_table_path"].items()
                )
            )

        if generate_sql:
            return script
        else:
            return self.execute_script(script)

    def step_03_create_source_view(self, generate_sql=False):
        if "query" in self.config["source"]:
            script = "CREATE OR REPLACE TEMPORARY VIEW __source_view AS \n {}".format(
                self.config["source"]["query"])
        else:
            script = "CREATE OR REPLACE TEMPORARY VIEW __source_view AS TABLE {} \n".format(
                self.config["source"]["table"])

        if generate_sql:
            return script
        else:
            return self.execute_script(script)

    def step_04_source_post_sql(self, generate_sql=False):
        if "post_sql" not in self.config["source"]:
            script = ""
        else:
            script = self.config["source"]["post_sql"]
        if generate_sql:
            return script
        else:
            return self.execute_script(script)

    def step_05_target_pre_sql(self, generate_sql=False):
        if "pre_sql" not in self.config["target"]:
            script = ""
        else:
            script = self.config["target"]["pre_sql"]
        if generate_sql:
            return script
        else:
            return self.execute_script(script)

    def step_06_operate(self, generate_sql=False):
        # TODO
        raise NotImplementedError

    def step_07_target_post_sql(self, generate_sql=False):
        if "post_sql" not in self.config["target"]:
            script = ""
        else:
            script = self.config["target"]["post_sql"]
        if generate_sql:
            return script
        else:
            return self.execute_script(script)

    def step_08_clean(self, generate_sql=False):
        # There is no need to remove temp views since they belong to a single session only.
        if generate_sql:
            return ""
        else:
            return None

    def generate_full_sql(self):
        """Generate job full sql"""
        # Changed on 24-03: job flow now contains
        #  + source pre-sql
        #  + script to create temp view to referenced tables
        #  + script to create create temp view related to source query
        #  + source post-sql
        #  + target pre-sql
        #  + operation happens
        #  + target post-sql
        #  + clean up: temp tables, temp views, etc

        scripts = [
            self.step_01_source_pre_sql(generate_sql=True),
            self.step_02_create_reference_views(generate_sql=True),
            self.step_03_create_source_view(generate_sql=True),
            self.step_04_source_post_sql(generate_sql=True),
            self.step_05_target_pre_sql(generate_sql=True),
            self.step_06_operate(generate_sql=True),
            self.step_07_target_post_sql(generate_sql=True),
            self.step_08_clean(generate_sql=True)
        ]
        return ";\n\n".join(filter(lambda x: x != "", scripts))

    def run(self):
        """Execute the whole job"""
        # Changed on 24-03: job flow now contains
        #  + source pre-sql
        #  + script to create temp view to referenced tables
        #  + script to create create temp view related to source query
        #  + source post-sql
        #  + target pre-sql
        #  + operation happens
        #  + target post-sql
        #  + clean up: temp tables, temp views, etc
        self.step_01_source_pre_sql(generate_sql=False)
        self.step_02_create_reference_views(generate_sql=False)
        self.step_03_create_source_view(generate_sql=False)
        self.step_04_source_post_sql(generate_sql=False)
        self.step_05_target_pre_sql(generate_sql=False)
        self.step_06_operate(generate_sql=False)
        self.step_07_target_post_sql(generate_sql=False)
        self.step_08_clean(generate_sql=False)
