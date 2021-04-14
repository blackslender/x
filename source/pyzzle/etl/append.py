from .parent import BaseETLJob


class AppendETLJob(BaseETLJob):
    def __init__(self, config, params={}):
        super(AppendETLJob, self).__init__(config, params=params)

        # Both 'insert' and 'append' operation are allowed
        assert self.config["target"]["operation"] in ["insert", "append"]

    def step_06_operate(self):
        if "table" in self.config["target"]:
            target_table = self.config["target"]["table"]
        elif "path" in self.config["target"]:
            target_table = "delta.`{}`".format(self.config["target"]["path"])

        source_df = self.spark.table("__source_view")
        if "table" in self.config["target"]:
            save_mode = "table"
            location = self.config["target"]["table"]

        elif "path" in self.config["target"]:
            save_mode = "path"
            location = self.config["target"]["path"]
        else:
            raise ETLJobException(
                "No location to write found in target configuration, please provide 'table' or 'path'."
            )

        return self.to_datasource.write(source_df,
                                        mode="append",
                                        location=location,
                                        save_mode=save_mode)
