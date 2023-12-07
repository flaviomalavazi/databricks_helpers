# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("temporary_gcs_bucket", "", "Temp GCS Bucket")
dbutils.widgets.text("target_bq_table", "", "Target bq table")
dbutils.widgets.dropdown("reset_data", "false", ["false", "true"], "Reset data")
dbutils.widgets.text("number_of_users", "1000", "Number of random users")
dbutils.widgets.text("sessions_per_day", "30000", "Number of sessions per day")
dbutils.widgets.text("simulation_duration", "120", "Simulation duration")

# COMMAND ----------

# MAGIC %pip install fake_web_events --disable-pip-version-check --quiet
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

temporary_gcs_bucket = dbutils.widgets.get("temporary_gcs_bucket")
target_bq_table = dbutils.widgets.get("target_bq_table")
write_mode = "overwrite" if dbutils.widgets.get("reset_data") == "true" else "overwrite"
number_of_users = int(dbutils.widgets.get("number_of_users"))
sessions_per_day = int(dbutils.widgets.get("sessions_per_day"))
simulation_duration =int(dbutils.widgets.get("simulation_duration"))

# COMMAND ----------

def generate_web_events(
    number_of_users=1,
    sessions_per_day=1,
    simulation_duration=1,
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
    number_of_users=number_of_users,
    sessions_per_day=sessions_per_day,
    simulation_duration=simulation_duration,
    temporary_gcs_bucket=temporary_gcs_bucket,
    target_bq_table=target_bq_table,
    write_mode = write_mode
)
