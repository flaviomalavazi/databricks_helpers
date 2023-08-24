# Databricks notebook source
source_catalog_incremental_table = "SOURCE_CLOUD_data_engineering"
source_schema_incremental_table = "cloud_migration_demo"
source_incremental_table = "dinners"

target_catalog_incremental_table = "TARGET_data_engineering"
target_schema_incremental_table = "cloud_migration_demo"
target_incremental_table = "dinners_incremental"
checkpoint_path = f"/Volumes/TARGET_CLOUD_data_engineering/cloud_migration_demo/streaming_checkpoints/{target_catalog_incremental_table}/{target_schema_incremental_table}/{target_incremental_table}"

# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_catalog_incremental_table};")
spark.sql(
    "CREATE SCHEMA IF NOT EXISTS"
    f" {target_catalog_incremental_table}.{target_schema_incremental_table};"
)

# COMMAND ----------

df = spark.readStream.table(
    f"{source_catalog_incremental_table}.{source_schema_incremental_table}.{source_incremental_table}"
)

# COMMAND ----------

(
    df.writeStream.trigger(availableNow=True)
    .option("checkpointLocation", checkpoint_path)
    .toTable(
        f"{target_catalog_incremental_table}.{target_schema_incremental_table}.{target_incremental_table}"
    )
)
