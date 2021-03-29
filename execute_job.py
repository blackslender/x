# This file shall be called from ADF to execute an ETL job
# Parameters:
#  + config_path: yml file path related to job config
#  **keyword arguments: shall be passed into job as parameters

import sys
import pyzzle
import json
import pyspark

if __name__ == "__main__":
    args = filter(lambda x: "=" not in x, sys.argv)
    kargs = dict(
        map(
            lambda x: (x[:x.index("=")], x[x.index("=")+1:]),
            filter(lambda x: "=" in x, sys.argv)
        )
    )
    json_kargs = json.dumps(kargs)

    config_path = kargs["config_path"]

    dataloader = pyzzle.DataLoader.init_dataloader(
        config_path,
        spark=pyspark.sql.session.SparkSession.getActiveSession(),
        params=kargs)
