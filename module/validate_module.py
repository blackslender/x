from functools import reduce
import module


class JobConfigException(Exception):
    pass


class JobConfigValidator:

    def __init__(self, dataloader, print_log=True):
        assert isinstance(dataloader, module.DataLoader)
        self.dataloader = dataloader

        if print_log:
            self.print = print
        else:
            self.print = lambda x: None

    def check_config_key(self, key, job_config=None, raise_exception=False):
        '''
        Check if a key exists in the config object
        Parameters:
        - key: str - the config key (ex: "source") or list - a series of nested key(ex: ["source", "query"])
        - job_config: dict of config object. If no job_config is provided, use the dataloader's config as default

        '''
        # If job_config is None, use dataloader's
        job_config = job_config or self.dataloader.config
        assert isinstance(key, str) or (isinstance(key, list)
                                        and all(map(lambda x: isinstance(x, str), key)))
        if isinstance(key, str):
            key = [key]
        nested_key = ""
        for k in key:
            if k not in job_config:
                if raise_exception:
                    raise JobConfigException(
                        "The key '{k}' does not exists in job_config{nested_key}")
                else:
                    return False
            job_config = job_config[k]
            nested_key += "[\"{}\"]".format(k)
        return True

    def check_multiple_config_keys(self, key_list, job_config=None, raise_exception=False):
        '''
        Check if all required keys exist in the config object
        Parameters:
        - key_list: the list of keys with:
            + key: str - the config key (ex: "source") or list - a series of nested key(ex: ["source", "query"])
        - job_config: dict of config object. If no job_config is provided, use the dataloader's config as default
        '''
        job_config = job_config or self.dataloader.config
        missing_keys = list(filter(lambda x: self.check_config_key(
            x, raise_exception=False), key_list))

        if len(missing_keys) > 0:
            if raise_exception:
                raise JobConfigException(
                    "{} keys are required in job config".format(missing_keys))
        return len(missing_keys) > 0

    def validate_all(self, raise_exception=False):
        test_result = True
        self.print("Start validating job config")
        self.print("Validating mandatory parameters...")
        test_result &= self.check_mandatory_param(
            raise_exception=raise_exception)
        self.print(
            "Validating mandatory parameters for UPDATE and UPSERT operations...")
        test_result &= self.check_mandatory_param_update_and_upsert(
            raise_exception=raise_exception)
        self.print("Validating target table's existant...")
        test_result &= self.check_table_exists(raise_exception=raise_exception)
        self.print("Successful validation!")
        return test_result

    def check_mandatory_param_update_and_upsert(self, job_config=None, raise_exception=False):
        '''
        Parameters:
            - job_config: dict of config object. If no config is provided, use the dataloader's config as default
        '''
        job_config = job_config or self.dataloader.config
        required_keys = [
            ["target", "update_column"],
            ["target", "primary_key_column"]
        ]

        if job_config["target"]["operation"].lower() not in ("update", "upsert"):
            return True

        try:
            return self.check_multiple_config_keys(required_keys, job_config=job_config, raise_exception=raise_exception)
        except JobConfigException as e:
            if raise_exception:
                raise JobConfigException(
                    "For UPDATE/UPSERT operation: " + str(e))
            else:
                return False
        raise NotImplementedError

    def check_mandatory_param(self, job_config=None, raise_exception=False):
        job_config = job_config or self.dataloader.config
        required_keys = [
            "source",
            "target",
            ["target", "table"],
            ["target", "operation"]
        ]
        if not self.check_multiple_config_keys(required_keys, job_config=job_config, raise_exception=raise_exception):
            return False

        if job_config["target"]["operation"] not in ("insert", "append", "update", "overwrite", "upsert"):
            if raise_exception:
                raise JobConfigException(
                    "Operation {} is not valid. The allowed operations are 'insert' or 'append', 'update', 'overwrite', 'upsert'".format(job_config["target"]["operation"]))
            else:
                return False
        return True

    def check_table_exists(self, job_config=None, raise_exception=False):
        '''
        Parameters:
            - job_config: dict of config object. If no config is provided, use the dataloader's config as default
        '''
        job_config = job_config or self.dataloader.config
        try:
            self.dataloader.execute_script("show create table {table_name} ;".format(
                table_name=job_config["target"]["table"]))
            return True
        except Exception as e:
            if "not found in database" in str(e):
                if raise_exception:
                    raise JobConfigException("Table {} does not exists. Please check the configuration.".format(
                        job_config["target"]["table"]))
                else:
                    return False
            else:
                raise e
