# Databricks notebook source
dbutils.widgets.removeAll()
## Widgets for notebooks 00 and 01
dbutils.widgets.text("temporary_gcs_bucket_bq_writer", "", "Temp GCS Bucket")
dbutils.widgets.text("target_bq_table_web_events", "", "Target bq table")
dbutils.widgets.text("target_catalog", "", "Target catalog")
dbutils.widgets.text("target_external_location", f"", "Target external location")
dbutils.widgets.text("target_schema", "", "Target schema")
dbutils.widgets.dropdown("reset_data", "false", ["false", "true"], "Reset data")
dbutils.widgets.text("lakehouse_federation_bigquery_catalog", "", "Federated BQ Catalog")
dbutils.widgets.text("number_of_users", "1000", "Number of random users")
dbutils.widgets.text("sessions_per_day", "30000", "Number of sessions per day")
dbutils.widgets.text("simulation_duration", "120", "Simulation duration")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_big_query_table = dbutils.widgets.get("target_bq_table_web_events")
reset_data = True if dbutils.widgets.get("reset_data") == 'true' else False
temporary_gcs_bucket = dbutils.widgets.get("temporary_gcs_bucket_bq_writer")
lakehouse_federation_bigquery_catalog = dbutils.widgets.get("lakehouse_federation_bigquery_catalog")
number_of_users = int(dbutils.widgets.get("number_of_users"))
sessions_per_day = int(dbutils.widgets.get("sessions_per_day"))
simulation_duration =int(dbutils.widgets.get("simulation_duration"))

dbutils.widgets.text("ref_bq_table", f"{lakehouse_federation_bigquery_catalog}.{target_big_query_table}", "Reference table")
dbutils.widgets.text("payments_path", f"/Volumes/{target_catalog}/{target_schema}/landing_database_events", "Where to put payment data?")
dbutils.widgets.text("payments_checkpoints", f"/Volumes/{target_catalog}/{target_schema}/streaming_checkpoints", "Where to store checkpoints")

## Widgets for notebook 02
big_query_federated_table = dbutils.widgets.get("ref_bq_table")
payments_path = dbutils.widgets.get("payments_path")
target_external_location = dbutils.widgets.get("target_external_location")
payments_checkpoint_path = dbutils.widgets.get("payments_checkpoints")
target_table_transactions = f"{target_catalog}.{target_schema}.tab_sale_transactions"
target_table_customers = f"{target_catalog}.{target_schema}.tab_customer_records"

## Widgets for notebook 03
target_bq_table_google = f"{target_catalog}.tab_google_ads"
target_table_f = f"{target_catalog}.{target_schema}.tab_facebook_investment"
target_table_b = f"{target_catalog}.{target_schema}.tab_bing_investment"
target_table_m = f"{target_catalog}.{target_schema}.tab_mailchimp_emails"

# COMMAND ----------

dbutils.notebook.run(path='./Data_Generators/00_web_events_data_generator', timeout_seconds=1200, arguments={
    "temporary_gcs_bucket": temporary_gcs_bucket,
    "target_bq_table": target_big_query_table,
    "reset_data": reset_data,
    "number_of_users": number_of_users,
    "sessions_per_day": sessions_per_day,
    "simulation_duration": simulation_duration,
})

# COMMAND ----------

dbutils.notebook.run(path='./Data_Generators/01_transaction_events_generator', timeout_seconds=1200, arguments={
    "target_bucket_path": payments_path,
    "checkpoints": payments_checkpoint_path,
    "target_catalog": target_catalog,
    "target_schema": target_schema,
    "target_table": target_table_transactions,
    "ref_bq_table": big_query_federated_table,
    "reset_data": reset_data,
})

# COMMAND ----------

dbutils.notebook.run(path='./Data_Generators/02_customer_records_generator', timeout_seconds=1200, arguments = {
    "target_catalog": target_catalog,
    "target_schema": target_schema,
    "target_table": target_table_customers,
    "ref_bq_table": big_query_federated_table,
    "reset_data": reset_data,
})

# COMMAND ----------

dbutils.notebook.run(path='./Data_Generators/03_media_data_generation', timeout_seconds=1200, arguments={
    "target_catalog": target_catalog,
    "target_schema": target_schema,
    "ref_bq_table": big_query_federated_table,
    "temporary_gcs_bucket": temporary_gcs_bucket,
    "target_bucket_path": target_external_location,
    "reset_data": reset_data,
    "target_bq_table_google": target_bq_table_google,
    "target_table_f": target_table_f,
    "target_table_b": target_table_b,
    "target_table_m": target_table_m,
})
