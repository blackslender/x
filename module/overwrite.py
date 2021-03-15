from parent import DataLoader


class DataLoaderAppend(DataLoader):

    def __init__(self, config, params={}):
        super(DataLoaderAppend, self).__init__(config, params)
        assert self.config["target"]["operation"] == "overwrite"

    def generate_main_script(self):
        main_sql = '''INSERT OVERWRITE {target} SELECT * FROM ({source_query})'''
        if self.config["target"]["create_staging_table"]:
            source_query = self._temp_table_name
        else:
            source_query = self.config['source']['query']
        main_sql = main_sql.format(
            target=self.config['target']['table'],
            source_query=source_query)
        return main_sql
