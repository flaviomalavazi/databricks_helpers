# Databricks notebook source
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

landing_path = "<LANDING_PATH>"
target_catalog = "hive_metastore" # As of 2023-03-24, the UC+Python DLT integration is not available, so we are defaulting to the hive_metastore
target_schema = "<TARGET_SCHEMA>"
bronze_table_name = "landing_sensor_data"
bronze_data_format = "json"
bronze_infer_column_types = "true"
silver_table_column_definition = "deviceId"

if bronze_data_format in ["json", "csv", "parquet"]:
    file_format = "cloudFiles"
    bronze_options = {
        "cloudFiles.format": bronze_data_format,
        "cloudFiles.inferColumnTypes": bronze_infer_column_types
    }
else:
    file_format = bronze_data_format
    bronze_options = {}

# COMMAND ----------

@dlt.table(
  name = bronze_table_name,
  comment="Raw table for sensor data"
)
def get_raw_sensor_data():
    return (
        spark.readStream
          .format(file_format)
          .options(**bronze_options)
          .load(landing_path)
    )