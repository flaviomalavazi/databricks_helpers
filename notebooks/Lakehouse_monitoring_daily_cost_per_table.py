# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("table_catalog", "", "Monitored Catalog")
dbutils.widgets.text("table_schema", "", "Monitored schema")
dbutils.widgets.text("days_lookback", "90", "Lookback window")

# COMMAND ----------

# MAGIC %pip install --upgrade databricks-sdk --quiet --disable-pip-version-check
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

table_catalog = dbutils.widgets.get("table_catalog")
table_schema = dbutils.widgets.get("table_schema")
days_lookback = dbutils.widgets.get("days_lookback")

# COMMAND ----------

from databricks.sdk import WorkspaceClient
from databricks.sdk.service import catalog

w = WorkspaceClient()

# COMMAND ----------

all_tables = w.tables.list(catalog_name=table_catalog, schema_name=table_schema)

table_info = []
for table in all_tables:
    table_info.append({
        "table_id": table.as_dict()["table_id"],
        "catalog_name": table.as_dict()["catalog_name"],
        "schema_name": table.as_dict()["schema_name"],
        "name": table.as_dict()["name"],
        "table_type": table.as_dict()["table_type"],
    })

df = spark.createDataFrame(table_info)

df.createOrReplaceTempView("tvw_table_ids")

# COMMAND ----------

df = spark.sql(f"""
                SELECT
                    us.usage_date,
                    us.custom_tags.LakehouseMonitoringTableId                   as table_id,
                    ti.catalog_name || "." || ti.schema_name || "." || ti.name  as table_full_name,
                    sum(us.usage_quantity)                                      as dbus,
                    dbus * any_value(lp.pricing.`default`)                      as monitoring_cost_at_list_price
                FROM
                    system.billing.usage us
                    left join system.billing.list_prices lp on lp.sku_name = us.sku_name and lp.price_end_time is null
                    left join tvw_table_ids ti on ti.table_id = us.custom_tags.LakehouseMonitoringTableId
                WHERE
                    us.usage_date >= DATE_SUB(current_date(), {days_lookback})
                    AND us.sku_name like "%JOBS_SERVERLESS%"
                    AND us.custom_tags ["LakehouseMonitoring"] = "true"
                GROUP BY
                    us.usage_date,
                    us.custom_tags.LakehouseMonitoringTableId,
                    ti.catalog_name,
                    ti.schema_name,
                    ti.name
          """)

# COMMAND ----------

display(df)
