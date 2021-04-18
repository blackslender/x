import pyzzle
from functools import reduce
import yaml
import re
from pyspark.sql import functions as F, types as T


def init_recon_job(config_yaml_filepath: str, params: dict = {}):
    '''Creates Recon job object related to configuration file.

    This method should be used to create Recon job objects instead of direct constructor.

    Args:
        config_yaml_filepath: Path to yaml config file. 
        params: 
            dict of (param_name: param_value) which is dynamic parameters for job config.
            Dynamic parameters could be placed in job config as '${param_name}'

    Returns:
        An Recon job object.

    Raises:
        Exception: Some parameter(s) are not provided.
        Exception: 'target - operation' key must exist in job config.
        Exception: Unexpected operation.
    '''

    with open(config_yaml_filepath, "r") as f:
        raw_config = f.read()
        for key in params:
            raw_config = raw_config.replace(f"${{{key}}}", params[key])
        config = yaml.safe_load(raw_config)

    # Make sure that all parameters are provided
    # TODO: move this to validation
    def get_required_params(text):
        param_ex = r"\$\{[A-Za-z_]+[A-Za-z0-9_]*\}"
        all_params = list(map(lambda x: x[2:-1], re.findall(param_ex, text)))
        return all_params

    if len(get_required_params(config_yaml_filepath)) > 0:
        raise Exception("All parameters should be provided. Please provide " +
                        str(get_required_params(config_yaml_filepath)))

    # All config key should be lowercase
    for key in list(config.keys()):
        config[key.lower()] = config[key]

    return ReconJob(config)


class ReconJob:
    def __init__(self, config_dict: dict, params: dict = dict()):
        # TODO:
        # - Validate config
        # - Create datasource objects related to each datasource
        #

        self.config = config_dict

        # Validation
        source_id = 0
        for source in self.config["data"]:
            if "name" not in source:
                source["name"] = "source_" + str(source_id)
            source_id += 1

        # Convert list of dict data to dict of {name: datadict}
        objs = dict()
        for obj in self.config["data"]:
            objs[obj["name"]] = obj
            del obj["name"]
        self.config["data"] = objs

        # For each source, convert metrics from list of dict to a single dict of (name: expr)
        for name in self.config["data"]:
            self.config["data"][name]["metrics"] = reduce(
                lambda x, y: {
                    **x,
                    **y
                }, self.config["data"][name]["metrics"])

    def step_01_query(self):
        # TODO:
        # - Get data from query/table/path
        # - Add a new attribute to current job self.datasource["name"] = DataSource object
        # - Add a new attribute to current job self.df["name"] = related dataframe
        self.datasource = dict()
        self.df = dict()
        for name, config in self.config["data"].items():
            # Create data source
            datasource = pyzzle.datasource.init_datasource(
                config["datasource"]
            )  # In the future, put other configurations here
            if "query" in config:
                df = datasource.sql(config["query"])
            elif "table" in config:
                df = datasource.table(config["table"])
            elif "path" in config:
                df = datasource.table(config["path"], mode="path")
            else:
                raise Exception(
                    "Either 'query', 'table' or 'path' should be provided in each recon's element"
                )

            self.datasource[name] = datasource
            self.df[name] = df

    def step_02_calculate(self):
        # TODO:
        # - Calculate group-by and metrics for each datasource
        # - It is simpler to calculate the group-by and aggregation using SQL syntax since the input is in SQL expressions
        # - Add a new attribute to current job self.agg["name"] = aggregated dataframe
        self.agg = dict()

        for name, config in self.config["data"].items():
            group_by = self.config["group_by"]
            agg = self.df[name].groupBy(*group_by)

            agg_expr = []
            for alias, expr in config["metrics"].items():
                agg_expr.append(F.expr(f"{expr} AS {alias}"))
            agg = agg.agg(*agg_expr)

            agg = agg.select(*(group_by + list(config["metrics"].keys())))
            self.agg[name] = agg

    def step_03_join(self):
        # TODO:
        # - Join all result of step_02 based on the group by attributes.
        # - For each metrics, renamed it to "datasource: metric_name"
        # - For each combination of datasource, calculate data difference column
        # - Calculate a test_result column if every related metric matches (If only 2 input sources is provided)
        group_by = self.config["group_by"]

        # Rename every metric with prefix as source_metricname
        for source, agg in self.agg.items():
            metric_cols = list(filter(lambda x: x not in group_by,
                                      agg.columns))
            self.agg[source] = reduce(
                lambda df, metric: df.withColumnRenamed(
                    metric, source + "_" + metric), metric_cols, agg)

        # Join
        joined = reduce(lambda x, y: x.join(y, how="full", on=group_by),
                        self.agg.values())

        # Calculate differences if there are only two sources
        if len(self.agg) == 2:
            source1, source2 = tuple(self.config["data"].keys())
            source1_metrics = list(
                self.config["data"][source1]["metrics"].keys())
            source2_metrics = list(
                self.config["data"][source2]["metrics"].keys())
            # Look for same metrics in both sources
            # I know that it could be done in O(n), this is more readable
            shared_metrics = sorted(
                set(source1_metrics) & set(source2_metrics))
            for metric in shared_metrics:
                try:
                    joined = joined.withColumn(
                        "delta_" + metric,
                        F.abs(
                            F.col(source1 + "_" + metric) -
                            F.col(source2 + "_" + metric)))
                except:  # Cannot calculate difference, eg in case the metric is string
                    pass
                # For float and double type, the acceptance rate is 0.1 percent
                if dict(joined.dtypes)[source1 + "_" + metric] in ("float", "double") \
                    or dict(joined.dtypes)[source2 + "_" + metric] in ("float", "double"):

                    def difference(number1, number2, error=1e-3):
                        return (number1 - number2) / number2 < error

                    joined = joined.withColumn(
                        "match_" + metric,
                        F.udf(difference,
                              T.BooleanType())(F.col(source1 + "_" + metric),
                                               F.col(source2 + "_" + metric)))
                else:
                    joined = joined.withColumn(
                        "match_" + metric,
                        F.col(source1 + "_" + metric) == F.col(source2 + "_" +
                                                               metric))
        self.joined = joined
        return joined

    def run(self):
        # TODO:
        # Execute all steps
        # Return the final comparison dataframe
        self.step_01_query()
        self.step_02_calculate()
        self.step_03_join()
