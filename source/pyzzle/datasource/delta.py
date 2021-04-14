from .base_datasource import BaseDataSource
import pyspark
from pyspark.sql import DataFrame
from functools import reduce
from delta.tables import DeltaTable


class DeltaDataSource(BaseDataSource):
    def __init__(self, spark_session: pyspark.sql.SparkSession = None):
        '''Delta Lake datasource

        Initialize an object representing the Delta Lake Storage datasource

        Args:
            spark_session: pyspark.sql.SparkSession.
                This is the spark session related to cluster runtime (spark variable on Databricks).
                If None is provided, it will try to get the active session.
        '''
        super(DeltaDataSource, self).__init__()
        if spark_session is None:
            # Try to get default session if None is provided
            spark_session = pyspark.sql.SparkSession.getActiveSession()
        if spark_session is None:
            raise datasource.DataSourceException(
                "Couldn't find an active Spark session. Please provide Spark session"
            )
        self.spark = spark_session
        self.format = "delta"

    def sql(self, query):
        '''Executes an SQL query from the source.

        Only atomic query (without semicolon) is allowed at the moment.

        Args:
            table_name: str, name of the table to get.
            path: str, location of the path to read.
        Return: Spark Dataframe that is the result of the query.
        '''
        super(DeltaDataSource, self).sql(query)
        return self.spark.sql(query)

    def table(self, location, mode="table"):
        '''Reads data from a table or path

        Only one of 'table_name' or 'path' must be provided.

        Args:
            location: str, name of the table to get or path to the table
            mode: str, 'table' or 'path'

        Returns: Spark Dataframe that represents the table or path

        Raises:
            DataSourceException: Either 'table' or 'path' parameter should be provided to read.
        '''
        mode = mode.lower()

        if mode == "table": return self.spark.table(location)
        elif mode == "path": return self.spark.read.load(location)
        else:
            raise datasource.DataSourceException(
                "mode should be either 'table' or 'path'/")

    def write(
            self,
            df: DataFrame,
            mode: str,
            location: str,  # Table name or path
            options: dict = {},
            save_mode: str = "table",  # 'table' or 'path'
    ):
        '''Write a dataframe to target physical table.

        This write operation can represent both append/insert and overwrite operation.

        Args:
            df: DataFrame, the source dataframe to write.
            mode: 'append'/'insert' or 'overwrite'
            location: table name (if save_mode is table) or path (if save_mode is path) to write to.
            options: dict, each key - value pair is a write option.
            save_mode: 'table' or 'path'
        '''
        super(DeltaDataSource, self).write(df,
                                           mode,
                                           location,
                                           options=options,
                                           save_mode=save_mode)
        mode = mode.lower()
        if mode == 'insert': mode = 'append'
        writer = df.write
        writer = writer.format(self.format)
        writer = writer.mode(mode)
        writer = reduce(lambda w, c: w.option(c, options[c]), options, writer)

        save_mode = save_mode.lower()
        if save_mode == "table":
            writer.saveAsTable(location)
        elif save_mode == "path":
            writer.save(location)
        else:
            raise datasource.DataSourceException("Invalid save_mode %s" % mode)

    def merge(
            self,
            df: DataFrame,
            location: str,
            condition: str,  # Only supports SQL-like string condition
            match_update_dict: dict,  # "target_column": "expression"
            insert_when_not_matched: False, # Set to True for upsert
            save_mode: str = 'table'):
        '''Merge a dataframe to target table or path.

        This merge operation can represent both update and upsert operation.
        Source and target table is defaultly alias-ed as 'SRC' and 'TGT'. This could be used in condition string and update/insert expressions.
        Args:
            df (DataFrame): The source dataframe to write.
            save_mode (str): 'table' or 'path'
            location (str): The table name or path to be merge into.
            condition (str): The condition in SQL-like string form.
            match_update_dict (dict): Contains ("target_column": "expression"). 
                This represents the updated value if matched.
                NOTE: "target_column"'s come without schema ("SRC" or "TGT").
            not_match_insert_dict (dict): Contains ("target_column": "expression"). 
                This represents the inserted value if not matched. 
                Other columns which are not specified shall be null.
                NOTE: "target_column"'s come without schema ("SRC" or "TGT").
        '''
        super(DeltaDataSource,
              self).merge(df,
                          condition,
                          match_update_dict,
                          insert=)
        save_mode = save_mode.lower()
        if save_mode == "table":
            target_table = DeltaTable.forName(self.spark, location)
        elif save_mode == "path":
            target_table = DeltaTable.forPath(self.spark, location)
        else:
            raise ValueError("save_mode should be 'path' or 'table'.")

        merger = target_table.alias("TGT").merge(df.alias("SRC"), condition)
        merger = merger.whenMatchedUpdate(set=match_update_dict)
        if insert_when_not_match:
            merger = merger.whenNotMatchedInsertAll()

        merger.execute()