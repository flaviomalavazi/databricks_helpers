# Databricks notebook source
dbutils.widgets.removeAll()
## Widgets for notebooks 00 and 01
dbutils.widgets.text("target_catalog", "", "Target catalog")
dbutils.widgets.text("target_schema", "", "Target schema")
dbutils.widgets.text("target_table_web_events", "tab_analytics_events", "Target web events table")
dbutils.widgets.dropdown("reset_data", "false", ["false", "true"], "Reset data")
dbutils.widgets.text("number_of_users", "10", "Number of random users")
dbutils.widgets.text("sessions_per_day", "3000", "Number of sessions per day")
dbutils.widgets.text("simulation_duration", "120", "Simulation duration")

# COMMAND ----------

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table_web_events = dbutils.widgets.get("target_table_web_events")
reset_data = True if dbutils.widgets.get("reset_data") == 'true' else False
number_of_users = int(dbutils.widgets.get("number_of_users"))
sessions_per_day = int(dbutils.widgets.get("sessions_per_day"))
simulation_duration =int(dbutils.widgets.get("simulation_duration"))

## Reference table for downstream notebooks (native Unity Catalog Delta table)
ref_web_events_table = f"{target_catalog}.{target_schema}.{target_table_web_events}"

## Widgets for notebook 01
payments_path = f"/Volumes/{target_catalog}/{target_schema}/landing_database_events"
payments_checkpoint_path = f"/Volumes/{target_catalog}/{target_schema}/streaming_checkpoints"
target_table_transactions = f"{target_catalog}.{target_schema}.tab_sale_transactions"
target_table_customers = f"{target_catalog}.{target_schema}.tab_customer_records"

## Widgets for notebook 03
target_table_g = f"{target_catalog}.{target_schema}.tab_google_ads"
target_table_f = f"{target_catalog}.{target_schema}.tab_facebook_investment"
target_table_b = f"{target_catalog}.{target_schema}.tab_bing_investment"
target_table_m = f"{target_catalog}.{target_schema}.tab_mailchimp_emails"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog} MANAGED LOCATION 's3://databricks-storage-7474649725642103/unity-catalog/7474649725642103/{target_catalog}'")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")

# COMMAND ----------

dbutils.notebook.run(path='./Data_Generators/00_web_events_data_generator', timeout_seconds=3600, arguments={
    "target_catalog": target_catalog,
    "target_schema": target_schema,
    "target_table": target_table_web_events,
    "reset_data": reset_data,
    "number_of_users": number_of_users,
    "sessions_per_day": sessions_per_day,
    "simulation_duration": simulation_duration,
})

# COMMAND ----------

dbutils.notebook.run(path='./Data_Generators/01_transaction_events_generator', timeout_seconds=1200, arguments={
    "path": payments_path,
    "checkpoints": payments_checkpoint_path,
    "target_catalog": target_catalog,
    "target_schema": target_schema,
    "target_table": target_table_transactions,
    "ref_web_events_table": ref_web_events_table,
    "reset_data": reset_data,
})

# COMMAND ----------

dbutils.notebook.run(path='./Data_Generators/02_customer_records_generator', timeout_seconds=1200, arguments = {
    "target_catalog": target_catalog,
    "target_schema": target_schema,
    "target_table": target_table_customers,
    "ref_web_events_table": ref_web_events_table,
    "reset_data": reset_data,
})

# COMMAND ----------

dbutils.notebook.run(path='./Data_Generators/03_media_data_generation', timeout_seconds=1200, arguments={
    "target_catalog": target_catalog,
    "target_schema": target_schema,
    "ref_web_events_table": ref_web_events_table,
    "reset_data": reset_data,
    "target_table_g": target_table_g,
    "target_table_f": target_table_f,
    "target_table_b": target_table_b,
    "target_table_m": target_table_m,
})
