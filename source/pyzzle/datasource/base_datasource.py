import pyzzle
import abc
import pyspark
import functools

from pyspark.sql import DataFrame


class DataSourceException(Exception):
    pass


def init_datasource(source_type, spark=None):  # Change later
    if source_type == "delta":
        if spark is None:
            spark = pyspark.sql.SparkSession.getActiveSession()
        if spark is None:
            raise DataSourceException(
                "For Delta datasource, please provide spark session that manages the Delta lake."
            )
        return pyzzle.datasource.DeltaDataSource(spark)
    else:
        raise DataSourceException(
            "source_type '{}' not found".format(source_type))


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
            insert_when_not_matched:
        dict = None,  # Leave None for update operation
    ):
        if type(self) is BaseDataSource:
            raise NotImplementedError
