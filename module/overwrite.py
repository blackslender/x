from module import DataLoader
import warnings


class DataLoaderOverwrite(DataLoader):

    def __init__(self, config, params={}):
        super(DataLoaderOverwrite, self).__init__(config, params)
        assert self.config["target"]["operation"] == "overwrite"

    def generate_main_script(self):
        warnings.warn(
            "OVERWRITE operation has not been supported yet. This query is for reference purpose only")
        main_sql = '''
INSERT OVERWRITE {target}
SELECT * FROM ({source_query})
REPLACE WHERE ({replace_where})
'''
        if self.config["target"]["create_staging_table"]:
            source_query = self._staging_table_name
        else:
            source_query = self.config['source']['query']
        main_sql = main_sql.format(
            target=self.config['target']['table'],
            source_query=source_query,
            replace_where=self.config["target"]["where_statement_on_table"] if "where_statement_on_table" in self.config["target"] else "1=1"
        )
        return main_sql

    # Since overwrite per partition is not yet supported, this is implemented using pyspark syntax
    def execute_main_script(self):
        """Generate and execute the main script"""
        # return self.execute_script(self.generate_main_script())
        if self.config["target"]["create_staging_table"]:
            source_table = spark.table(self._staging_table_name)
        else:
            source_table = spark.sql(self.config['source']['query'])
        source_table.write\
            .format("delta") \
            .mode("overwrite") \
            .option("replaceWhere", self.config["target"]["where_statement_on_table"] if "where_statement_on_table" in self.config["target"] else "1 = 1") \
            .saveAsTable(self.config["target"]["table"])
