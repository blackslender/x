import pyzzle
import abc
import pyspark
import functools

from pyspark.sql import DataFrame


class DataSourceException(Exception):
    pass


class BaseDataSource(abc.ABC):
    def __init__(self):
        self.name = "datasource_name"
        if type(self) is BaseDataSource:
            raise NotImplementedError

    def sql(self, script: str):
        if type(self) is BaseDataSource:
            raise NotImplementedError

    def table(self, table_name):
        if type(self) is BaseDataSource:
            raise NotImplementedError

    def write(
            self,
            df: DataFrame,
            mode: str,
            location: str,  # Table name or path
            options: dict = {},
            save_mode: str = "table",  # 'table' or 'path'
    ):
        if type(self) is BaseDataSource:
            raise NotImplementedError

    def merge(
        self,
        df: DataFrame,
        condition: str,  # Only support SQL-like string condition
        match_update_dict: dict,  # "target_column": "expression"
        not_match_insert_dict: dict = None  # Leave None for update operation
    ):
        if type(self) is BaseDataSource:
            raise NotImplementedError
