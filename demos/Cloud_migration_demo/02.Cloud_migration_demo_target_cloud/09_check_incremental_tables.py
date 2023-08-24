# Databricks notebook source
# MAGIC %sql
# MAGIC select max(dinner_id) as latest_dinner from SOURCE_CLOUD_data_engineering.cloud_migration_demo.dinners

# COMMAND ----------

# MAGIC %sql
# MAGIC select max(dinner_id) as latest_dinner from TARGET_CLOUD_data_engineering.cloud_migration_demo.dinners_incremental
