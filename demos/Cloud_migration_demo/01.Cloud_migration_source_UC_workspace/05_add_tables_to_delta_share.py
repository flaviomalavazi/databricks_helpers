# Databricks notebook source
# DBTITLE 1,Definindo Widgets
dbutils.widgets.text("share_name", "MY_SHARE", "01- Source Share Name")
dbutils.widgets.text("source_catalog", "SOURCE_CLOUD", "02- Source Catalog Name")
dbutils.widgets.text(
    "mapping_schema", "__cloud_migration_map", "03- Source Mapping Schema"
)

# COMMAND ----------

# DBTITLE 1,Definindo parâmetros e variáveis
share_name = (
    dbutils.widgets.get("share_name")
    if dbutils.widgets.get("share_name") != ""
    else "MY_SHARE"
)
source_catalog = (
    dbutils.widgets.get("source_catalog")
    if dbutils.widgets.get("source_catalog") != ""
    else "SOURCE_CLOUD"
)
mapping_schema = (
    dbutils.widgets.get("mapping_schema")
    if dbutils.widgets.get("mapping_schema") != ""
    else "__cloud_migration_map"
)

# COMMAND ----------

# DBTITLE 1,Lendo todos os schemas que devem ser compartilhados com a outra cloud
schemas = [
    x.schema_name
    for x in spark.sql(
        f"SELECT DISTINCT schema_name FROM {source_catalog}.information_schema.schemata"
    ).collect()
]

# COMMAND ----------

# DBTITLE 1,Acrescentando todos os schemas escolhidos ao share cross-cloud
for my_schema in schemas:
    if my_schema not in [mapping_schema, "information_schema", "default"]:
        spark.sql(f"""ALTER SHARE {share_name} ADD SCHEMA {source_catalog}.{my_schema}""")
    else:
        print(
            f"Skipped schema: {source_catalog}.{my_schema} because it's a default or"
            " ignored schema"
        )
