# Databricks notebook source
# DBTITLE 1,Importando bibliotecas
from datetime import datetime

import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Obtendo os nomes dos schemas que precisaremos varrer
# Forçando o resto do código a utilizar o hive metastore como catálogo (ambiente sem Unity Catalog)
spark.sql("USE CATALOG hive_metastore")
SCHEMAS = spark.sql("SHOW SCHEMAS")
my_schema_list = SCHEMAS.select(F.collect_list("databaseName")).first()[0]

# COMMAND ----------

# DBTITLE 1,Varrendo os schemas para obter os nomes das tabelas que vamos investigar
table_map = {}
for schema in my_schema_list[0:10]:  # Limitado a 10 schemas para exemplo
    tables = spark.sql(f"SHOW TABLES IN {schema}")
    this_table_list = tables.select(F.collect_list("tableName")).first()[0]
    table_map[schema] = this_table_list
table_map = [
    item
    for row in [
        [{"table_schema": k, "table_name": v} for v in vs] for k, vs in table_map.items()
    ]
    for item in row
]
print(f"Vou fazer a busca de coordenadas de {len(table_map)} tabelas")

# COMMAND ----------

# DBTITLE 1,Passando schema a schema, tabela a tabela, obtendo o seu tamanho
print(f"Terminei a busca de coordenadas as:\t{datetime.now()}")
for table in table_map:
    try:
        table_location = (
            spark.sql(
                f"""DESCRIBE DETAIL `hive_metastore`.`{table['table_schema']}`.`{table['table_name']}`"""
            )
            .select(F.collect_list("location"))
            .first()[0][0]
        )
        table["table_location"] = table_location
    except Exception as e:
        print(f"Exception {e}")
        table["location"] = None
print(f"Terminei a busca de coordenadas as:\t{datetime.now()}")

# COMMAND ----------

# DBTITLE 1,Montando um dataframe com a lista de tabelas e suas localizações
df_schema = StructType(
    [
        StructField("table_schema", StringType(), False),
        StructField("table_name", StringType(), False),
        StructField("table_location", StringType(), False),
    ]
)

# creating a dataframe
dataframe = (
    spark.createDataFrame(table_map, schema=df_schema)
    .withColumn("created_at", F.lit(datetime.now()))
    .withColumn("updated_at", F.lit(datetime.now()))
)
dataframe.createOrReplaceTempView("current_table_map")

# COMMAND ----------

# DBTITLE 1,Montando uma tabela com o resumo das coordenadas para definirmos a tabela no Unity Catalog e criarmos o Delta Sharing
mapping_schema = "__migration_map"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {mapping_schema}")

mapping_table = "tab_lake_map"
export_mount = "dbfs:/mnt/demo_storage_account"
table_location = f"{export_mount}/{mapping_schema}/{mapping_table}"

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {mapping_schema}.{mapping_table} (table_schema STRING, table_name STRING, table_location STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
LOCATION '{table_location}'
"""
)

spark.sql(
    f"""MERGE INTO {mapping_schema}.{mapping_table} as target USING current_table_map as source
  ON (target.table_schema = source.table_schema AND target.table_name = source.table_name AND target.table_location != source.table_location)
  WHEN MATCHED THEN UPDATE SET target.table_location = source.table_location, target.updated_at = source.updated_at
  WHEN NOT MATCHED THEN INSERT *;"""
)

# COMMAND ----------

print(f"Table map location (for mapping on the UC enabled workspace): {table_location}")

# COMMAND ----------

# DBTITLE 1,Amostra das tabelas
spark.read.table(f"{mapping_schema}.{mapping_table}").limit(10).display()

# COMMAND ----------

# DBTITLE 1,Criando um dataframe com o mapa de mounts (para reconstruirmos as tabelas no workspace com UC)
from collections import ChainMap

mounts = [
    {f"dbfs:{mount.mountPoint}": mount.source}
    for mount in dbutils.fs.mounts()
    if mount.mountPoint.startswith("/mnt")
]
mounts_dict = dict(ChainMap(*mounts))
mounts = [[key, val[:-1]] for key, val in mounts_dict.items()]

df_mounts_schema = StructType(
    [
        StructField("mount_point", StringType(), True),
        StructField("mount_location", StringType(), True),
    ]
)

dataframe_mounts = (
    spark.createDataFrame(mounts, schema=df_mounts_schema)
    .withColumn("created_at", F.lit(datetime.now()))
    .withColumn("updated_at", F.lit(datetime.now()))
)
dataframe_mounts.createOrReplaceTempView("current_mount_map")

# COMMAND ----------

# DBTITLE 1,Montando uma tabela com o resumo dos mounts para definirmos as tabelas do Unity Catalog
mapping_schema = "__migration_map"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {mapping_schema}")

mapping_table = "tab_mounts_map"
export_mount = "dbfs:/mnt/demo_storage_account"
table_location = f"{export_mount}/{mapping_schema}/{mapping_table}"

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {mapping_schema}.{mapping_table} (mount_point STRING, mount_location STRING, created_at TIMESTAMP, updated_at TIMESTAMP)
LOCATION '{table_location}'
"""
)

spark.sql(
    f"""MERGE INTO {mapping_schema}.{mapping_table} as target USING current_mount_map as source
  ON (target.mount_point = source.mount_point AND target.mount_location != source.mount_location)
  WHEN MATCHED THEN UPDATE SET target.mount_location = source.mount_location, target.updated_at = source.updated_at
  WHEN NOT MATCHED THEN INSERT *;"""
)

# COMMAND ----------

print(
    "Mounts map location (for mapping on the UC enabled workspace):"
    f" {mounts_dict[export_mount]}{mapping_schema}/{mapping_table}"
)

# COMMAND ----------

# DBTITLE 1,Mostrando os resultados
spark.read.table(f"{mapping_schema}.{mapping_table}").limit(10).display()
