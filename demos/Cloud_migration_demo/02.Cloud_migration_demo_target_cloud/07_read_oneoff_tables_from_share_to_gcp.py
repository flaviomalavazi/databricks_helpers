# Databricks notebook source
# DBTITLE 1,Definindo widgets
dbutils.widgets.text(
    "delta_sharing_catalog",
    "SOURCE_CLOUD_data_engineering",
    "01- Delta Sharing Source Catalog",
)
dbutils.widgets.text("target_catalog", "TARGET_data_engineering", "02- Target Catalog")

# COMMAND ----------

# DBTITLE 1,Parâmetros e variáveis
delta_sharing_catalog = (
    dbutils.widgets.get("delta_sharing_catalog")
    if dbutils.widgets.get("delta_sharing_catalog") != ""
    else "SOURCE_CLOUD_data_engineering"
)
target_catalog = (
    dbutils.widgets.get("target_catalog")
    if dbutils.widgets.get("target_catalog") != ""
    else "TARGET_data_engineering"
)

# COMMAND ----------

# DBTITLE 1,Import de bibliotecas
import pyspark.sql.functions as F

# COMMAND ----------

# DBTITLE 1,Listando todos os schemas compartilhados no Share para que façamos a criação das tabelas na nova cloud
schemas = [
    schema.databaseName
    for schema in spark.sql(f"SHOW SCHEMAS IN {delta_sharing_catalog}").collect()
    if schema.databaseName not in ["information_schema", "default"]
]

# COMMAND ----------

# DBTITLE 1,Listando as tabelas dos schemas do Share
table_map = {}
for my_schema in schemas[0:10]:  # Limitado a 10 schemas para exemplo
    tables = spark.sql(f"SHOW TABLES IN {delta_sharing_catalog}.{my_schema}")
    this_table_list = tables.select(F.collect_list("tableName")).first()[0]
    table_map[my_schema] = this_table_list
table_map = [
    item
    for row in [
        [{"table_schema": k, "table_name": v} for v in vs] for k, vs in table_map.items()
    ]
    for item in row
]
print(f"Vou fazer a busca de coordenadas de {len(table_map)} tabelas")

# COMMAND ----------

# DBTITLE 1,Criando as tabelas na nova Cloud a partir dos dados da antiga, habilitando o Delta Uniform
spark.sql("set spark.databricks.delta.write.dataFilesToSubdir = true")
for my_table in table_map:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_catalog}.{my_table['table_schema']};")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {target_catalog}.{my_table['table_schema']}.{my_table['table_name']} USING DELTA TBLPROPERTIES (
        'delta.columnMapping.mode' = 'name',
        'delta.universalFormat.enabledFormats' = 'iceberg'
        ) AS SELECT * FROM {delta_sharing_catalog}.{my_table['table_schema']}.{my_table['table_name']}
    """
    )

# COMMAND ----------

# DBTITLE 1,Montando a lista de manifestos para registrar as tabelas como externas no BQ
import requests
host = "https://"+dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().get()
api_endpoint = "api/2.1/unity-catalog/tables/"
headers = {"Authorization": "Bearer "+dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().get()}
table_metadata = []

for my_table in table_map:
    try:
        table_metadata.append([f"{target_catalog}.{my_table['table_schema']}.{my_table['table_name']}", dict(requests.get(url=f"{host}/{api_endpoint}/{target_catalog}.{my_table['table_schema']}.{my_table['table_name']}", headers=headers).json())["delta_uniform_iceberg"]["metadata_location"]])
    except:
        table_metadata.append([f"{target_catalog}.{my_table['table_schema']}.{my_table['table_name']}", None])

schema = StructType([
  StructField('table_name', StringType(), True),
  StructField('metadata_location', StringType(),True)
])

df = spark.createDataFrame(data=table_metadata, schema = schema)
