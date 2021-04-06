import datasource
import pyspark
from pyspark.sql import DataFrame
from functools import reduce


class DeltaDataSource(datasource.BaseDataSource):
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

    def execute_sql(self, query):
        '''Executes an SQL query from the source.

        Only atomic query (without semicolon) is allowed at the moment.

        Args:
            table_name: str, name of the table to get.
            path: str, location of the path to read.
        Return: Spark Dataframe that is the result of the query.
        '''
        super(DeltaDataSource, self).execute_sql(query)
        return self.spark.sql(query)

    def read(self, table: str = None, path: str = None):
        '''Reads data from a table or path

        Only one of 'table_name' or 'path' must be provided.

        Args:
            table_name: str, name of the table to get
            path: str, location of the path to read

        Returns: Spark Dataframe that represents the table or path

        Raises:
            DataSourceException: Either 'table' or 'path' parameter should be provided to read.
        '''
        if table is None and path is None:
            raise datasource.DataSourceException(
                "Either 'table' or 'path' parameter should be provided to read."
            )

        if table is not None: return self.spark.table(table)
        else: return self.spark.read.load(path)

    def write(
            self,
            df: DataFrame,
            mode: str,
            target: str,  # Table name or path
            options: dict = {},
            save_mode: str = "table",  # 'table' or 'path'
    ):
        '''Write a dataframe to target physical table.

        This write operation can represent both append/insert and overwrite operation.

        Args:
            df: DataFrame, the source dataframe to write.
            mode: 'append'/'insert' or 'overwrite'
            target: table name (if save_mode is table) or path (if save_mode is path) to write to.
            options: dict, each key - value pair is a write option.
            save_mode: 'table' or 'path'
        '''
        super(DeltaDataSource, self).write(df,
                                           mode,
                                           target,
                                           options=options,
                                           save_mode=save_mode)
        mode = mode.lower()
        if mode == 'insert': mode = 'append'
        writer = df.write
        writer = writer.format(self.format)
        writer = writer.mode(mode)
        writer = reduce(lambda w, c: w.option(c, options[c]))

        save_mode = save_model.lower()
        if save_mode == "table":
            writer.saveAsTable(target)
        elif save_mode == "path":
            writer.save(path)
        else:
            raise datasource.DataSourceException("Invalid save_mode %s" % mode)

    def merge(
        self,
        df: DataFrame,
        target: str,
        condition: str,  # Only support SQL-like string condition
        match_update_dict: dict,  # "target_column": "expression"
        not_match_insert_dict: dict = None  # Leave None for update operation
    ):
        '''Merge a dataframe to target table.

        This merge operation can represent both update and upsert operation.

        Args:
            df: DataFrame, the source dataframe to write.
            target: str, the table name or path to be merge into.
            condition: str, the condition in SQL-like string form.
            match_update_dict: dict, contains ("target_column": "expression"). This represents the updated value if matched.
            not_match_insert_dict: dict, contains ("target_column": "expression"). This represents the inserted value if not matched. Other columns which are not specified shall be null.
        '''
        #TODO
