# Databricks notebook source
# MAGIC %md
# MAGIC # Pausing a continous job
# MAGIC This notebooks has a working example on how to create a notebook that pauses a continous job. This is useful for when we have different data arrival SLAs during a day/weekend, and it allows us to manage costs. 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Installing the databricks python SDK

# COMMAND ----------

# MAGIC %pip install databricks-sdk==0.16.0 --quiet --disable-pip-version-check
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Importing the necessary items to perform the task at hand

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import jobs
from databricks.sdk.service.jobs import Continuous, JobSettings, PauseStatus
w = WorkspaceClient()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Defining the job we want to pause/unpause

# COMMAND ----------

my_continuous_job_id = "YOUR_JOB_ID"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Getting the current configs for the job we want to edit and altering its status based on the current config

# COMMAND ----------

current_job_settings = w.jobs.get(my_continuous_job_id).as_dict()

# COMMAND ----------

try:
    if current_job_settings["settings"]["continuous"]["pause_status"] == "PAUSED":
        print("The job will be resumed")
        new_status = "UNPAUSED"
    else:
        print("The job will be paused")
        new_status = "PAUSED"
except Exception as e:
    raise e

# COMMAND ----------

# MAGIC %md
# MAGIC ### Performing the update

# COMMAND ----------

update_result = w.jobs.update(job_id=my_continuous_job_id, new_settings=JobSettings(continuous=Continuous(PauseStatus(new_status))))
