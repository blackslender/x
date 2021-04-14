from .parent import BaseETLJob

def generate_merge_condition(primary_key_list, base_condition):
    if base_condition is None: base_condition = "1=1"
    link_char = " AND "
    return base_condition \
        + link_char \
        + link_char.join(map(lambda x: "TGT.{} = SRC.{}".format(x, x),  + list_of_column))

def merge(self, insert_when_not_matched):
    if "table" in self.config["target"]:
        target_table = self.config["target"]["table"]
    elif "path" in self.config["target"]:
        target_table = "delta.`{}`".format(self.config["target"]["path"])

    if "where_statement_on_table" not in job_config["target"]:
        self.config["target"]["where_statement_on_table"] = "1=1"
    
    merge_condition = generate_merge_condition(
        self.config["target"]["primary_key_column"],
        self.config["target"]["where_statement_on_table"]
    )

    return self.to_datasource.merge(
        self.spark.table("__source_view"),
        target_table,
        condition=merge_condition,
        match_update_dict = dict(map(lambda x: (x,x), self.config["target"]["update_column"])),
        insert_when_not_matched = insert_when_not_matched
    )

class UpdateETLJob(BaseETLJob):
    def __init__(self, config, params={}):
        super(UpdateETLJob, self).__init__(config, params=params)
        assert self.config["target"]["operation"] == "update"
        assert "primary_key_column" in self.config["target"]
        assert "update_column" in self.config["target"]

    def step_06_operate(self):
        return merge(self, insert_when_not_matched=False)


class UpsertETLJob(BaseETLJob):
    def __init__(self, config, params={}):
        super(UpsertETLJob, self).__init__(config, params=params)
        assert self.config["target"]["operation"] == "upsert"
        assert "primary_key_column" in self.config["target"]
        assert "update_column" in self.config["target"]

    def step_06_operate(self):
        return merge(self, insert_when_not_matched=True)
        