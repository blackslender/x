import pyzzle
from functools import reduce
import yaml
import re
import pyspark


class BaseETLJob:
    @staticmethod
    def init_etl_job(config_yaml_filepath: str, params: dict = {}):
        '''Creates ETL job object related to configuration file.

        This method should be used to create ETL job objects instead of direct constructor.

        Args:
            config_yaml_filepath: Path to yaml config file. 
            params: 
                dict of (param_name: param_value) which is dynamic parameters for job config.
                Dynamic parameters could be placed in job config as '${param_name}'

        Returns:
            An ETL job object (one of its sub-classes object, related to the config's operation).

        Raises:
            Exception: Some parameter(s) are not provided.
            Exception: 'target - operation' key must exist in job config.
            Exception: Unexpected operation.
        '''

        with open(config_yaml_filepath, "r") as f:
            raw_config = f.read()
            for key in params:
                raw_config = raw_config.replace(f"${{{key}}}", params[key])
            config = yaml.safe_load(raw_config)

        # Make sure that all parameters are provided
        # TODO: move this to validation
        def get_required_params(text):
            param_ex = r"\$\{[A-Za-z_]+[A-Za-z0-9_]*\}"
            all_params = list(
                map(lambda x: x[2:-1], re.findall(param_ex, text)))
            return all_params

        if len(get_required_params(config_yaml_filepath)) > 0:
            raise Exception(
                "All parameters should be provided. Please provide " +
                str(get_required_params(config_yaml_filepath)))

        # All config key should be lowercase
        for key in list(config.keys()):
            config[key.lower()] = config[key]

        try:
            operation = config["target"]["operation"]
        except KeyError as e:
            raise KeyError("The target - operation key is required for a job.")

        if operation.lower() == "overwrite":
            return pyzzle.etl.OverwriteETLJob(config, params=params)
        if operation.lower() in ["append", "insert"]:
            return pyzzle.etl.AppendETLJob(config, params=params)
        elif operation.lower() == "update":
            return pyzzle.etl.UpdateETLJob(config, params=params)
        elif operation.lower() == "upsert":
            return pyzzle.etl.UpsertETLJob(config, params=params)
        else:
            raise ValueError("Unexpected oparation '%s'" % operation)

    def __init__(self, config, params={}):
        r'''DO NOT USE CONSTRUCTOR TO CREATE JOB . 
        
        Instead, use static 'init_etl_job' as an object factory.
        When overwriting this constructor, the parent constructor should be called as super(ChildETLJob, self).\_\_init\_\_(config, params)

        Args:
            config: dict of job config, which is parsed from yml config file.
            params: 
                dict of (param_name: param_value) which is dynamic parameters for job config.
                Dynamic parameters could be placed in job config as '${param_name}'
        
        Returns: None

        Raises:
            Exception: BaseETLJob class is abstract.
        '''
        if type(self) is BaseETLJob:
            raise Exception("BaseETLJob class is abstract.")

        self.config = config
        self.version = self.config["version"]
        self.params = params

        if self.config["source"]["datasource"].lower() == "delta":
            self.from_datasource = pyzzle.datasources.DeltaDataSource()
        else:
            raise pyzzle.datasources.DataSourceException(
                "Datasource %s not found" %
                self.config["config"]["datasource"])

        if self.config["source"]["datasource"].lower() == "delta":
            self.from_datasource = pyzzle.datasources.DeltaDataSource()
        else:
            raise pyzzle.datasources.DataSourceException(
                "Datasource %s not found" %
                self.config["config"]["datasource"])

        # TODO: Complete the validation module
        # For now, it's temporarily disabled
        # pyzzle.JobConfigValidator(
        #     self, print_log=False).validate_all(raise_exception=True)

    def validate(self):
        '''Manually validates the job

        The validation responsibility belongs to `pyzzle.JobConfigValidator` class. This method simply call validation and print all result to stdout

        '''
        return pyzzle.JobConfigValidator(self, print_log=True).validate_all()

    def __repr__(self):
        return str(self.config)

    # Script executor

    def execute_script(self, script):
        '''Executes an sql-script from databricks' sparksession.
        
        Execute an sql-script from spark session. Users are responsible to validate this script.
        If a multi-statement script is provided, it would be split into atomic scripts by semicolon (;) and these would be executed sequently.

        Args:
            script: the sql-script
        
        Returns: 
            A SparkDataframe which is the result of the script. If script is multi-statement, return the result of lastest atomic sub-script.
            Return None if empty script is provided.
        
        '''

        # Users are responsible to validate the script
        script = script.replace("\n", " ")
        if script.replace(" ", "") == "":
            return None
        if ";" not in script:
            return self.spark.sql(script)
        else:
            # If a multi-statement script is provided, return the result of last statement.
            statements = filter(lambda x: x != "",
                                map(lambda x: x.strip(), script.split(";")))

            return list(map(lambda x: self.spark.sql(x), statements))[-1]

    def step_01_source_pre_sql(self, generate_sql=False):
        '''Executes job source side pre-sql
        
        Step 1 of the job's process. If pre-sql is provided from job's source side, it would be executed ON THE SOURCE SIDE

        Args:
            generate_sql: boolean. If yes, only return the sql-script related to this step. Else, execute the step immediately.
        
        Returns: 
            If generate_sql=True: return the sql-script related to this step (without executing).
            Else: return the pre-sql query result as SparkDataFrame
        '''
        if "pre_sql" not in self.config["source"]:
            script = ""
        else:
            script = self.config["source"]["pre_sql"]

        if generate_sql:
            return script
        else:
            return self.from_datasource.execute_sql_script(script)

    def step_02_create_reference_views(self, generate_sql=False):
        ''' Creates temp views related to reference table paths from source config
        
        If reference_table_path is provided from job's source side, they're be created as temporary views IN SPARK SESSION. 
        This step is only available with delta datasource.

        Args:
            generate_sql: boolean. If yes, only return the sql-script related to this step. Else, execute the step immediately.
        
        Returns: 
            If generate_sql=True: return the sql-script related to this step (without executing).
            Else: return dictionary of (view_name: view_dataframe)
        '''
        def create_view_ddl(args):
            view_name, path = args
            return "CREATE OR REPLACE TEMPORARY VIEW {} AS SELECT * FROM delta.`{}`;".format(
                view_name, path)

        if "reference_table_path" not in self.config["source"]:
            script = ""
        else:
            if isinstance(self.config["source"]["reference_table_path"], list):
                r = dict()
                for subdict in self.config["source"]["reference_table_path"]:
                    r = {**r, **subdict}
                self.config["source"]["reference_table_path"] = r

            script = ";\n".join(
                map(create_view_ddl,
                    self.config["source"]["reference_table_path"].items()))

        # TODO: Modify this so it shall support other sources
        if generate_sql:
            return script
        else:
            return self.execute_script(script)

    def step_03_create_source_view(self, generate_sql=False):
        ''' Creates temp view represents the source query

        This method create (or replace) a temp view called '__source_view' that represents the source query or source table.

        Args:
            generate_sql: boolean. If yes, only return the sql-script related to this step. Else, execute the step immediately.
        
        Returns: 
            If generate_sql=True: return the sql-script related to this step (without executing).
            Else: return the pre-sql query result as SparkDataFrame. Since creating temp view doesn't result anything, this would return None

        Parameters:
            + paths: dictionary of (view_name, table path)
            + generate_sql: bool. If False: execute creating the view, else return the sql script only (no execution).
        Return: 
            dictionary of (view_name, dataframe) if generate_sql=True else str - the script'''
        script = ("--Spark Session\n")
        if "query" in self.config["source"]:
            script += "CREATE OR REPLACE TEMPORARY VIEW __source_view AS \n {}".format(
                self.config["source"]["query"])
        else:
            script += "CREATE OR REPLACE TEMPORARY VIEW __source_view AS TABLE {}"\
                .format(self.config["source"]["table"])

        if generate_sql:
            return script
        else:
            source_df = self.from_datasource.execute_sql_script(
                self.config["source"]["query"])
            source_df.createOrReplaceTempView("__source_view")

    def step_04_source_post_sql(self, generate_sql=False):
        '''Executes job source side post-sql
        
        Step 4 of the job's process. If post-sql is provided from job's source side, it would be executed ON THE SOURCE SIDE.
        Args:
            generate_sql: boolean. If yes, only return the sql-script related to this step. Else, execute the step immediately.
        
        Returns: 
            If generate_sql=True: return the sql-script related to this step (without executing).
            Else: return the post-sql query result as SparkDataFrame
        '''
        if "post_sql" not in self.config["source"]:
            script = ""
        else:
            script = ("--%s\n" % self.from_datasource.name) \
                + self.config["source"]["post_sql"]
        if generate_sql:
            return script
        else:
            return self.from_datasource.execute_sql_script(script)

    def step_05_target_pre_sql(self, generate_sql=False):
        '''Executes job target side pre-sql
        
        Step 5 of the job's process. If pre-sql is provided from job's target side, it would be executed ON THE TARGET SIDE.

        Args:
            generate_sql: boolean. If yes, only return the sql-script related to this step. Else, execute the step immediately.
        
        Returns: 
            If generate_sql=True: return the sql-script related to this step (without executing).
            Else: return the pre-sql query result as SparkDataFrame
        '''
        if "pre_sql" not in self.config["target"]:
            script = ""
        else:
            script = ("--%s\n" % self.to_datasource.name
                      ) + self.config["target"]["pre_sql"]
        if generate_sql:
            return script
        else:
            return self.to_datasource.execute_script(script)

    def step_06_operate(self, generate_sql=False):
        """TODO: Override this method based on the operation."""
        raise NotImplementedError

    def step_07_target_post_sql(self, generate_sql=False):
        '''Executes job source side post-sql
        
        Step 7 of the job's process. If post-sql is provided from job's target side, it would be executed ON THE TARGET SIDE.
        Args:
            generate_sql: boolean. If yes, only return the sql-script related to this step. Else, execute the step immediately.
        
        Returns: 
            If generate_sql=True: return the sql-script related to this step (without executing).
            Else: return the post-sql query result as SparkDataFrame
        '''
        if "post_sql" not in self.config["target"]:
            script = ""
        else:
            script = ("--%s\n" % self.to_datasource.name
                      ) + self.config["target"]["post_sql"]
        if generate_sql:
            return script
        else:
            return self.to_datasource.execute_sql_script(script)

    def step_08_clean(self, generate_sql=False):
        # There is no need to remove temp views since they belong to a single session only.
        if generate_sql:
            return ""
        else:
            return None

    def generate_full_sql(self):
        '''Generate job full sql'''
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
        return ";\n\n".join(filter(lambda x: x != "", scripts)) + ";"

    def run(self):
        '''Execute the whole job'''
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
