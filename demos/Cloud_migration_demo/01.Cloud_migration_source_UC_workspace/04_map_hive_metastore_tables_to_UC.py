# Databricks notebook source
# DBTITLE 1,Definição dos widgets
dbutils.widgets.text(
    "target_mapping_catalog", "SOURCE_CLOUD", "01- Target Mapping Catalog"
)
dbutils.widgets.text(
    "target_mapping_schema", "__cloud_migration_map", "02- Target Mapping Schema"
)
dbutils.widgets.text("target_mapping_table", "tab_lake_map", "03- Target Mapping Tables")
dbutils.widgets.text("target_mounts_table", "tab_mounts_map", "04- Target Mapping Mounts")

dbutils.widgets.text(
    "source_mapping_table_location",
    "abfss://CONTAINER@STORAGE_ACCOUNT.dfs.core.windows.net/__migration_map/tab_lake_map",
    "05- Source Mapping Table Location",
)  # Preencher manualmente
dbutils.widgets.text(
    "source_mapping_mounts_location",
    "abfss://CONTAINER@STORAGE_ACCOUNT.dfs.core.windows.net/__migration_map/tab_mounts_map",
    "06- Source Mounts Table Location",
)  # Preencher manualmente

# COMMAND ----------

# DBTITLE 1,Definição de parâmetros e variáveis
target_mapping_catalog = (
    dbutils.widgets.get("target_mapping_catalog")
    if dbutils.widgets.get("target_mapping_catalog") != ""
    else "azure"
)
target_mapping_schema = (
    dbutils.widgets.get("target_mapping_schema")
    if dbutils.widgets.get("target_mapping_schema") != ""
    else "__cloud_migration_map"
)
target_mapping_table = (
    dbutils.widgets.get("target_mapping_table")
    if dbutils.widgets.get("target_mapping_table") != ""
    else "tab_lake_map"
)
target_mounts_table = (
    dbutils.widgets.get("target_mounts_table")
    if dbutils.widgets.get("target_mounts_table") != ""
    else "tab_mounts_map"
)

source_mapping_table_location = (
    dbutils.widgets.get("source_mapping_table_location")
    if dbutils.widgets.get("source_mapping_table_location") != ""
    else None
)
source_mapping_mounts_location = (
    dbutils.widgets.get("source_mapping_mounts_location")
    if dbutils.widgets.get("source_mapping_mounts_location") != ""
    else None
)

# COMMAND ----------

# DBTITLE 1,Import de bibliotecas
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Criando o catálogo e schema que serão alvos para as tabelas de mapeamento estas tabelas (mount e tables)
spark.sql(f"CREATE CATALOG IF NOT EXISTS {target_mapping_catalog};")
spark.sql(f"USE CATALOG {target_mapping_catalog};")
spark.sql(
    f"CREATE SCHEMA IF NOT EXISTS {target_mapping_catalog}.{target_mapping_schema};"
)
spark.sql(
    "CREATE TABLE IF NOT EXISTS"
    f" {target_mapping_catalog}.{target_mapping_schema}.{target_mapping_table} LOCATION"
    f" '{source_mapping_table_location}';"
)
spark.sql(
    "CREATE TABLE IF NOT EXISTS"
    f" {target_mapping_catalog}.{target_mapping_schema}.{target_mounts_table} LOCATION"
    f" '{source_mapping_mounts_location}';"
)

# COMMAND ----------

# DBTITLE 1,Listando os mounts do outro workspace e seus respectivos caminhos absolutos e definindo uma UDF para tratar os paths
current_mounts = spark.sql(
    "select distinct mount_point, mount_location from"
    f" {target_mapping_catalog}.{target_mapping_schema}.{target_mounts_table}"
)
mount_list = [[x.mount_point, x.mount_location] for x in current_mounts.collect()]


def get_absolute_path(mount_list, location):
    if location.startswith("abfss://"):
        return location
    else:
        try:
            for mount in mount_list:
                location = location.replace(mount[0], mount[1])
            return location
        except Exception as e:
            print(e)
            return None


get_absolute_path_udf = udf(lambda x: get_absolute_path(mount_list, x), StringType())

# COMMAND ----------

# DBTITLE 1,Acrescentando o caminho absoluto de cada uma das tabelas que temos no workspace produtivo
current_tables = spark.read.table(
    f"{target_mapping_catalog}.{target_mapping_schema}.{target_mapping_table}"
).withColumn("absolute_path", get_absolute_path_udf("table_location"))

# COMMAND ----------

# DBTITLE 1,Criando as tabelas externas no Unity Catalog (e schemas necessários) para fazermos o compartilhamento posterior
for my_table in current_tables.collect():
    if (
        (my_table.table_schema != target_mapping_schema)
        and (my_table.table_name != target_mapping_table)
        and (my_table.table_name != target_mounts_table)
    ):
        print(
            "Creating table:"
            f" {target_mapping_catalog}.{my_table.table_schema}.{my_table.table_name} in"
            f" {my_table.absolute_path}"
        )
        spark.sql(
            f"""CREATE SCHEMA IF NOT EXISTS {target_mapping_catalog}.{my_table.table_schema};"""
        )
        spark.sql(
            f"""CREATE TABLE IF NOT EXISTS {target_mapping_catalog}.{my_table.table_schema}.{my_table.table_name} LOCATION '{my_table.absolute_path}';"""
        )
    else:
        print(
            f"Skipped the dictionary table: {my_table.table_schema}.{my_table.table_name}"
        )
