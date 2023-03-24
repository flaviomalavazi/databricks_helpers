# Databricks notebook source
import re
import dlt
from pyspark.sql.functions import *

# COMMAND ----------

landing_path = "<LANDING_PATH>"
target_catalog = "hive_metastore" 
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

all_tables = [x[silver_table_column_definition] for x in spark.sql(f"SELECT distinct {silver_table_column_definition} FROM {target_catalog}.{target_schema}.{bronze_table_name}").collect()]
all_tables = [[re.sub('[^A-Za-z0-9]+', '_', x.replace(" ", "_")), x] for x in all_tables]

# COMMAND ----------

def generate_table(live_table: str, filter_parameter: str, silver_table_column_definition: str):
    @dlt.table(
      name= live_table,
      comment="Silver custom data capture for " + live_table
    )
    def create_live_table():
        return (
                spark.readStream.
                    format("delta").
                    table(f"{target_catalog}.{target_schema}.{bronze_table_name}").
                    where(f"{silver_table_column_definition} = '{filter_parameter}'")
               )


[generate_table(live_table=c[0], filter_parameter = c[1], silver_table_column_definition = silver_table_column_definition) for c in all_tables]