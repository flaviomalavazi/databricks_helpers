# Databricks notebook source
dbutils.widgets.text("target_catalog", "", "01- Target catalog")
dbutils.widgets.text("target_schema", "", "01- Target schema")
dbutils.widgets.text("target_table", "", "01- Target table")

# COMMAND ----------

# MAGIC %pip install databricks-sdk --quiet --disable-pip-version-check
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from pyspark.sql.types import *
import pyspark.sql.functions as F
from datetime import datetime

# COMMAND ----------

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog};")
spark.sql(f"USE CATALOG {target_catalog};")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema};")
spark.sql(f"USE SCHEMA {target_schema};")

# COMMAND ----------

w = WorkspaceClient()

# COMMAND ----------

current_jobs = []
for job in w.jobs.list():
    current_jobs.append(job.as_dict())

# COMMAND ----------

job_attributes = StructType([
    StructField("job_id", LongType(), False),
    StructField("creator_user_name", StringType(), True),
    StructField("created_at", StringType(), True),
    StructField("job_name", StringType(), True),
    StructField("run_as", StringType(), True),
    StructField("schedule_quartz_cron_expression", StringType(), True),
    StructField("schedule_timezone_id", StringType(), True),
    StructField("pause_status", StringType(), True)
])

# COMMAND ----------

job_details = []
for job in current_jobs:
    my_job = {}
    my_job["job_id"] = job.get("job_id")
    my_job["created_at"] = job.get("created_time")
    my_job["creator_user_name"] = str(job.get("creator_user_name"))
    my_job["job_name"] = job["settings"].get("name")
    my_job["run_as"] = job["settings"].get("run_as")
    my_job["schedule_quartz_cron_expression"] = job["settings"].get("schedule", {"quartz_cron_expression": None}).get("quartz_cron_expression")
    my_job["schedule_timezone_id"] = job["settings"].get("schedule", {"timezone_id": None}).get("timezone_id")
    my_job["pause_status"] = job["settings"].get("schedule", {"pause_status": None}).get("pause_status")
    job_details.append(my_job)

# COMMAND ----------

spark.createDataFrame(job_details, schema=job_attributes).withColumn(
    "first_etl_at", F.lit(datetime.now())
).withColumn("etl_updated_at", F.lit(datetime.now())).createOrReplaceTempView(
    "incoming_data"
)

# COMMAND ----------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}.{target_table} (job_id LONG, creator_user_name STRING, created_at STRING, job_name STRING, run_as STRING, schedule_quartz_cron_expression STRING, schedule_timezone_id STRING, pause_status STRING, first_etl_at TIMESTAMP, etl_updated_at TIMESTAMP)
"""
)

# COMMAND ----------

spark.sql(
    f"""MERGE INTO { target_catalog }.{ target_schema }.{ target_table } as target USING incoming_data as source ON (
  target.job_id = source.job_id
  AND (
    target.creator_user_name != source.creator_user_name
    OR target.created_at != source.created_at
    OR target.job_name != source.job_name
    OR target.run_as != source.run_as
    OR target.schedule_quartz_cron_expression != source.schedule_quartz_cron_expression
    OR target.schedule_timezone_id != source.schedule_timezone_id
    OR target.pause_status != source.pause_status
  )
)
WHEN MATCHED THEN
UPDATE
SET
    target.creator_user_name = source.creator_user_name,
    target.created_at = source.created_at,
    target.job_name = source.job_name,
    target.run_as = source.run_as,
    target.schedule_quartz_cron_expression = source.schedule_quartz_cron_expression,
    target.schedule_timezone_id = source.schedule_timezone_id,
    target.pause_status = source.pause_status,
    target.etl_updated_at = source.etl_updated_at
  WHEN NOT MATCHED THEN
INSERT 
  *;"""
)
