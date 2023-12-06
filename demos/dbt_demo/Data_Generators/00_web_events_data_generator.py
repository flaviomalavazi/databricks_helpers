# Databricks notebook source
# dbutils.widgets.removeAll()
dbutils.widgets.text("temporary_gcs_bucket", "", "Temp GCS Bucket")
dbutils.widgets.text("target_bq_table", "", "Target bq table")
dbutils.widgets.dropdown("write_mode", "append", ["append", "overwrite"], "Write mode")

# COMMAND ----------

# MAGIC %pip install fake_web_events --disable-pip-version-check --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

temporary_gcs_bucket = dbutils.widgets.get("temporary_gcs_bucket")
target_bq_table = dbutils.widgets.get("target_bq_table")
write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------

def generate_web_events(
    number_of_users=1000,
    sessions_per_day=300000,
    simulation_duration=120,
    temporary_gcs_bucket=None,
    target_bq_table=None,
    write_mode="append",
):

    from fake_web_events import Simulation
    from datetime import datetime

    print(f"Generating web events...\t{datetime.now()}")
    simulation = Simulation(
        user_pool_size=number_of_users, sessions_per_day=sessions_per_day
    )
    events = simulation.run(duration_seconds=simulation_duration)

    list_of_events = []

    for event in events:
        list_of_events.append(event)
    print(f"Events generated...\t\t{datetime.now()}")
    df = spark.createDataFrame(list_of_events)
    print(f"Writing to BigQuery...\t\t{datetime.now()}")
    (
        df.write
            .format("bigquery")
            .mode(write_mode)
            .option("temporaryGcsBucket", temporary_gcs_bucket)
            .option("table", target_bq_table)
            .save()
    )
    print(f"All Done!\t\t\t{datetime.now()}")
    return df

# COMMAND ----------

df = generate_web_events(
    temporary_gcs_bucket=temporary_gcs_bucket,
    target_bq_table=target_bq_table,
)
