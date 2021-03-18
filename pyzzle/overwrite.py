from pyzzle import DataLoader
import warnings


class DataLoaderOverwrite(DataLoader):

    def __init__(self, config, spark=None, params={}):
        super(DataLoaderOverwrite, self).__init__(
            config, spark=spark, params=params)
        assert self.config["target"]["operation"] == "overwrite"

    def _get_target_table_partition_columns(self, table_name):
        try:
            return self.execute_script("SHOW PARTITIONS " + table_name).columns
        except Exception as e:
            if "not partitioned" in str(e):
                return []
            else:
                raise e

    def _generate_key_matching_condition_string(self):
        if self.config["target"]["create_staging_table"]:
            source_table = self.spark.table(self._staging_table_name)
        else:
            source_table = self.execute_script(self.config["source"]["query"])
        partition_columns = self._get_target_table_partition_columns(
            self.config["target"]["table"])
        distinct_partition_values = list(map(lambda x: x.asDict(
        ), source_table.select(*partition_columns).distinct().collect()))
        condition_string = " OR ".join(map(lambda row: "(" + " AND ".join(map(
            lambda key: "{key} = '{value}'".format(key=key, value=row[key]), row)) + ")", distinct_partition_values))
        if condition_string == "":
            condition_string = "1=1"
        return condition_string

    def generate_main_script(self):
        warnings.warn(
            "OVERWRITE operation has not been supported yet. This query is for reference purpose only")
        main_sql = '''
INSERT OVERWRITE {target}
SELECT * FROM ({source_query})
PARTITION ON ({primary_key_columns})
'''
        if self.config["target"]["create_staging_table"]:
            source_query = self._staging_table_name
        else:
            source_query = self.config['source']['query']
        main_sql = main_sql.format(
            target=self.config['target']['table'],
            source_query=source_query,
            primary_key_columns=str(self._get_target_table_partition_columns(
                self.config["target"]["table"])))
        return main_sql

    # Since overwrite per partition is not yet supported, this is implemented using pyspark syntax
    def execute_main_script(self):
        """Execute overwrite operation"""
        # return self.execute_script(self.generate_main_script())
        if self.config["target"]["create_staging_table"]:
            source_table = self.spark.table(self._staging_table_name)
        else:
            source_table = self.execute_script(self.config['source']['query'])
        condition_string = self._generate_key_matching_condition_string()
        source_table.write\
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", condition_string) \
            .saveAsTable(self.config["target"]["table"])


# Get partition columns
# If no partition: Overwrite all
# else:
#   Generate select distinct from source table on partition columns
#   Overwrite the distinct values selected on partition columns
