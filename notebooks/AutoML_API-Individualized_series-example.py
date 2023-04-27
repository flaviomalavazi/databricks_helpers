# Databricks notebook source
# MAGIC %md
# MAGIC # AutoML forecasting example
# MAGIC ## Requirements
# MAGIC Databricks Runtime for Machine Learning **13.0** or above.

# COMMAND ----------

# MAGIC %sh
# MAGIC pip install unidecode --quiet --disable-pip-version-check

# COMMAND ----------

dbutils.widgets.text("source_table_catalog", "hive_metastore", "01 - Source Data Catalog")                                     # Source data table catalog
dbutils.widgets.text("source_table_schema", "fsi_flavio_malavazi", "02 - Source Data Schema")                                  # Source data table schema
dbutils.widgets.text("source_table_name", "merchant_credit_card_transactions", "03 - Source Data Table")                       # Source data table name
dbutils.widgets.text("time_column", "timestamp", "04 - Timestamp column")                                                      # Column that identifies the time column in the Dataset
dbutils.widgets.text("series_identifier_columns", "merchant_type,card_network,merchant_name", "05 - Time series identifiers")  # Comma separated list of columns that will identify each independent series
dbutils.widgets.text("target_catalog", "hive_metastore", "06 - Target Data Catalog")                                           # Target catalog (to store the forecasted values and the run report)
dbutils.widgets.text("target_schema", "automl_flavio_malavazi", "07 - Target Data Schema")                                     # Target schema
dbutils.widgets.text("target_forecast_table_name", "tab_forecast_results", "08 - Target Data Table")                           # Target table for forecast values. The run report will be stored at `target_table_report`)
dbutils.widgets.text("target_col", "bill_value", "09 - Target Column")                                                         # Target forecast column
dbutils.widgets.text("prevision_horizon", "30", "10 - Prevision Horizon")                                                      # Prediction horizon (integer)
dbutils.widgets.dropdown("prevision_frequency", "S", ["Y", "Q", "M", "W", "D", "h", "m", "S"], "11 - Prevision Frequency")     # Years, Quarters, Months, Weeks, Days, hours, minutes, Seconds
dbutils.widgets.dropdown("primary_metric", "mdape", ["smape", "mse", "rmse", "mae", "mdape"], "12 - Primary Metric")           # Metric to be used "smape"(default) for AutoML "mse", "rmse", "mae", or "mdape"
dbutils.widgets.text("contry_code", "BR", "13 - Country Code")                                                                 # Country code to be considered for holidays

# COMMAND ----------

import pyspark.pandas as ps
import pandas as pd
from unidecode import unidecode
from typing import List
from datetime import datetime
from mlflow import pyfunc
from mlflow.tracking import MlflowClient
import re
import random
import string
import databricks.automl

spark.conf.set("spark.sql.execution.arrow.enabled","true")
ps.set_option("compute.ops_on_diff_frames", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Loading the parameters to read the full dataset
# MAGIC The first step is to read the full dataset in order to breakdown each forecast series into its own table

# COMMAND ----------

source_table_catalog = dbutils.widgets.get("source_table_catalog")                                                                          # "hive_metastore"
source_table_schema = dbutils.widgets.get("source_table_schema")                                                                            # "fsi_flavio_malavazi"
source_table_name = dbutils.widgets.get("source_table_name")                                                                                # "merchant_credit_card_transactions"

time_column = dbutils.widgets.get("time_column")                                                                                            # "timestamp"
series_identifier_columns = dbutils.widgets.get("series_identifier_columns").split(",")                                                     # ["merchant_type", "card_network", "merchant_name"]

target_catalog = dbutils.widgets.get("target_catalog")                                                                                      # "hive_metastore"
target_schema = dbutils.widgets.get("target_schema")                                                                                        # "automl_flavio_malavazi"
target_forecast_table_name = f"{dbutils.widgets.get('target_forecast_table_name')}_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S%f')}"   # f"tab_forecast_results_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S%f')}"

target_col = dbutils.widgets.get("target_col")                                                                                              # "bill_value"
prevision_horizon = int(dbutils.widgets.get("prevision_horizon"))                                                                           # 30
prevision_frequency = dbutils.widgets.get("prevision_frequency")                                                                            # "S"
primary_metric = dbutils.widgets.get("primary_metric")                                                                                      # "mdape"
contry_code = dbutils.widgets.get("contry_code")                                                                                            # "BR"

print("Input:")
print("\tSource Data:")
print(f"\t\t- Table at: {source_table_catalog}.{source_table_schema}.{source_table_name}")
print("\tParameters: ")
print(f"\t\t- Time identifier: {time_column}")
print(f"\t\t- Time series identifiers: {series_identifier_columns}")
print(f"\t\t- Forecast horizon: {prevision_horizon}")
print(f"\t\t- Forecast frequency: {prevision_frequency}")
print(f"\t\t- Primary evaluation metric: {primary_metric}")
print(f"\t\t- Country code holidays: {contry_code}")
print("")
print("Output:")
print("\tTarget Data:")
print(f"\t\t- Table at: {target_catalog}.{target_schema}.{target_forecast_table_name}")
print("\tRun report:")
print(f"\t\t- Execution report at: {target_catalog}.{target_schema}.{target_forecast_table_name}_report")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading the source dataset into a pandas on spark dataframe

# COMMAND ----------

df = ps.read_table(f"{source_table_catalog}.{source_table_schema}.{source_table_name}")

# COMMAND ----------

df.columns

# COMMAND ----------

automl_tables = df.groupby(series_identifier_columns).count().reset_index()[series_identifier_columns]

automl_tables["automl_table_identifier"] = automl_tables.apply(
    lambda x: f"automl_tab_{''.join(random.choices(string.ascii_lowercase, k=10))}",
    axis=1,
)

automl_tables["automl_table_filter"] = ps.DataFrame(
    (
        ps.DataFrame(
            [
                automl_tables.loc[
                    :, automl_tables.columns != "automl_table_identifier"
                ].columns
            ]
            * len(
                automl_tables.loc[:, automl_tables.columns != "automl_table_identifier"]
            ),
            columns=automl_tables.loc[
                :, automl_tables.columns != "automl_table_identifier"
            ].columns,
        )
        + """ = \""""
        + automl_tables.loc[
            :, automl_tables.columns != "automl_table_identifier"
        ].astype(str)
        + """\""""
    ).apply(" AND ".join, axis=1)
)

# COMMAND ----------

automl_tables.head(10)

# COMMAND ----------

# MAGIC %md
# MAGIC ## AutoML training
# MAGIC The following command starts an AutoML run. You must provide the column that the model should predict in the `target_col` argument and the time column. 
# MAGIC When the run completes, you can follow the link to the best trial notebook to examine the training code.  
# MAGIC
# MAGIC - **[Documentation for calling the AutoML API](https://docs.databricks.com/machine-learning/automl/train-ml-model-automl-api.html#forecasting)**
# MAGIC
# MAGIC As we will be conducting several independant AutoML experiments, it is important to monitor the duration of this notebook, as this might not be the most efficient way to forecast (filters can be added to avoid starting experiments for keys that have too little observations).
# MAGIC
# MAGIC This example also specifies:
# MAGIC - `horizon=30` to specify that AutoML should forecast 30 days into the future. 
# MAGIC - `frequency="d"` to specify that a forecast should be provided for each day. 
# MAGIC - `primary_metric="mdape"` to specify the metric to optimize for during training.

# COMMAND ----------

# MAGIC %md
# MAGIC # 🚨 Attention! 🚨
# MAGIC The cell bellow is running the experiment for a sample of the original dataset (only the 50 first combinations of parameters)
# MAGIC It takes a while for all experiments to run their independant AutoML runs so caution is advisable when using this notebook

# COMMAND ----------

import logging

# Disable informational messages from fbprophet
logging.getLogger("py4j").setLevel(logging.WARNING)

results_columns = [
    "automl_table_identifier",
    "best_model_uri",
    "successfull",
    "error",
]
automl_results = ps.DataFrame(columns=results_columns)

for index, row in automl_tables.head(50).iterrows():
    tempDF = spark.sql(
        f"SELECT * FROM {source_table_catalog}.{source_table_schema}.{source_table_name} WHERE {row['automl_table_filter']}"
    )
    try:
        summary = databricks.automl.forecast(
            tempDF,
            identity_col=series_identifier_columns,
            target_col=target_col,
            time_col=time_column,
            horizon=prevision_horizon,
            frequency=prevision_frequency,
            primary_metric=primary_metric,
            country_code=contry_code,
        )


        run_id = MlflowClient()
        trial_id = summary.best_trial.mlflow_run_id
        automl_results = automl_results.append(
            ps.DataFrame(
                [
                    [
                        row["automl_table_identifier"],
                        "runs:/{run_id}/model".format(run_id=trial_id),
                        True,
                        "",
                    ]
                ],
                columns=results_columns,
            ),
            ignore_index=True,
        )
    except Exception as e:
        automl_results = automl_results.append(
            ps.DataFrame(
                [[row["automl_table_identifier"], "", False, str(e)]],
                columns=results_columns,
            ),
            ignore_index=True,
        )


automl_tables = (
    automl_tables.set_index("automl_table_identifier")
    .join(
        automl_results.set_index("automl_table_identifier"),
        how="left",
    )
    .reset_index()
)

automl_tables["processed_at"] = datetime.now()
del automl_results

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load predictions from the best models for each series and forecast the data looking forward
# MAGIC We load the models for each of the best runs and forecast the series appending it to a single dataframe that will contain all the results

# COMMAND ----------

forecasts = {}
for index, row in automl_tables[automl_tables.successfull == True].iterrows():
    try:
        pyfunc_model = pyfunc.load_model(row["best_model_uri"])
        forecast = pyfunc_model._model_impl.python_model.predict_timeseries()
        forecast["forecasted_at"] = datetime.strftime(datetime.now(), "%Y-%m-%dT%H:%M:%S.%f")
        forecasts[row["automl_table_identifier"]] = forecast
    except Exception as e:
        print(e)

automl_tables = automl_tables.to_spark()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Forecasting data using the models that we created
# MAGIC Creating forecasts for each model we created and saving them to a dataframe with all the results

# COMMAND ----------

forecast_df = pd.DataFrame()
for automl_identifier, forecast_results in forecasts.items():
    forecast_df = pd.concat([forecast_df, forecast_results])

safe_column_names = [re.sub('[^A-Za-z0-9 _]+', '', unidecode(column)).replace(" ", "_").lower() for column in forecast_df.columns]
forecast_df.columns = safe_column_names
try:
    forecast_df = spark.createDataFrame(forecast_df)
except IndexError:
    print("No forecasts made")
    automl_tables.limit(10).display()
except Exception as e:
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### Saving the results

# COMMAND ----------

try:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
except Exception as e:
    raise e

# COMMAND ----------

try:
    forecast_df.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        f"{target_catalog}.{target_schema}.{target_forecast_table_name}"
    )
    automl_tables.write.mode("overwrite").option("mergeSchema", "true").saveAsTable(
        f"{target_catalog}.{target_schema}.{target_forecast_table_name}_report"
    )
    print(
        f"Forecast table saved successfully at: {target_catalog}.{target_schema}.{target_forecast_table_name}\n"
        f"Forecast report saved successfully at: {target_catalog}.{target_schema}.{target_forecast_table_name}_report"
    )
except Exception as e:
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ## Next steps
# MAGIC * Explore the notebooks and experiments linked above.
# MAGIC * If the metrics for the best trial notebook look good, you can continue with the next cell.
# MAGIC * If you want to improve on the model generated by the best trial:
# MAGIC   * Go to the notebook with the best trial and clone it.
# MAGIC   * Edit the notebook as necessary to improve the model.
# MAGIC   * When you are satisfied with the model, note the URI where the artifact for the trained model is logged. Assign this URI to the `model_uri` variable in the next cell.
