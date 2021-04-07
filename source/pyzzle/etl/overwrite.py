from .parent import BaseETLJob
import warnings


class OverwriteETLJob(BaseETLJob):
    def __init__(self, config, spark=None, params={}):
        super(OverwriteETLJob, self).__init__(config,
                                              spark=spark,
                                              params=params)
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
        source_table = self.execute_script("SELECT  * FROM __source_view")
        partition_columns = self._get_target_table_partition_columns(
            self.config["target"]["table"])
        distinct_partition_values = list(
            map(lambda x: x.asDict(),
                source_table.select(*partition_columns).distinct().collect()))
        condition_string = " OR ".join(
            map(
                lambda row: "(" + " AND ".join(
                    map(
                        lambda key: "{key} = '{value}'".format(
                            key=key, value=row[key]), row)) + ")"
                if len(row) > 0 else " 1=1 ", distinct_partition_values))
        if condition_string == "()":
            condition_string = " 1=1 "
        return condition_string

    def step_06_operate(self, generate_sql=False):
        if "table" in self.config["target"]:
            target_table = self.config["target"]["table"]
        elif "path" in self.config["target"]:
            target_table = "delta.`{}`".format(target_table)
        else:
            raise KeyError(
                "Either 'table' or 'path' key should appear in target config.")

        if generate_sql:
            partition_cols = self._get_target_table_partition_columns(
                target_table)
            script = [
                "-- OVERWRITE operation is not supported in Databricks SQL. These query are for reference only."
            ]
            script = [
                "INSERT OVERWRITE {target_table} PARTITION BY ({partition_cols}) SELECT * FROM __source_view"
                .format(target_table=target_table,
                        partition_cols=", ".join(partition_cols))
            ]
            return "\n".join(script)
        else:
            source_table = self.execute_script("SELECT * FROM __source_view")
            condition_string = self._generate_key_matching_condition_string()
            source_table.write\
                .format("delta") \
                .mode("overwrite") \
                .option("replaceWhere", condition_string) \
                .saveAsTable(self.config["target"]["table"])
