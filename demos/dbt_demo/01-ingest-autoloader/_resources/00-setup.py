# Databricks notebook source
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("target_catalog", "main", "Target Catalog")
dbutils.widgets.text("target_schema", "default", "Target Schema")
dbutils.widgets.text("source_volume", "dbt_demo", "Source Volume")

# COMMAND ----------

reset_all_data = dbutils.widgets.get("reset_all_data") == "true"
target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
source_volume = dbutils.widgets.get("source_volume")
raw_data_location = f"/Volumes/{target_catalog}/{target_schema}/{source_volume}/dbdemos/dbt-retail"
folder = f"/Volumes/{target_catalog}/{target_schema}/{source_volume}/dbdemos/dbt-retail"

print("00-setup notebook widget values:")
print(f"reset_all_data = {reset_all_data}")
print(f"target_catalog = {target_catalog}")
print(f"target_schema = {target_schema}")
print(f"source_volume = {source_volume}")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{target_schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {target_catalog}.{target_schema}.{source_volume};")

#Return true if the folder is empty or does not exists
def is_folder_empty(folder):
  try:
    return len(dbutils.fs.ls(folder)) == 0
  except:
    return True

if reset_all_data or is_folder_empty(folder+"/orders") or is_folder_empty(folder+"/users") or is_folder_empty(folder+"/events"):
  #data generation on another notebook to avoid installing libraries (takes a few seconds to setup pip env)
  print(f"Generating data under {folder} , please wait a few sec...")
  path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
  parent_count = path[path.rfind("01-ingest-autoloader"):].count('/') - 1
  prefix = "./" if parent_count == 0 else parent_count*"../"
  prefix = f'{prefix}_resources/'
  print("Running the data generator notebook")
  dbutils.notebook.run(prefix+"01-load-data", 600, {"reset_all_data": reset_all_data, "target_catalog": target_catalog, "target_schema": target_schema, "source_volume": source_volume})
else:
  print("data already existing. Run with reset_all_data=true to force a data cleanup for your local demo.")
  dbutils.notebook.exit()
