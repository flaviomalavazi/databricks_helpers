# Databricks notebook source
# # # Or if you need a more specific date boundaries, provide the start
# # # and end dates explicitly.
import datetime
from datetime import timedelta
from random import randrange

from pyspark.sql.functions import *
from pyspark.sql.types import DateType


def return_random_date() -> datetime:
    start_date = datetime.date(year=2023, month=1, day=1)
    end_date = datetime.datetime.today().date()
    delta = (end_date - start_date).days
    return start_date + timedelta(days=randrange(0, delta))


date_define = udf(return_random_date, DateType())

# COMMAND ----------

# catalog = "hive_metastore" #spark.sql("SELECT current_user()").head(1)[0].asDict()['current_user()'].split("@")[0].replace(".", "_")
# spark.sql(f"USE CATALOG {catalog}")

target_catalog = "hive_metastore"
target_schema = "cloud_migration_demo"

dinner_price = spark.read.table(f"{target_catalog}.{target_schema}.dinner_price")
available_recipes = (
    spark.read.table(f"{target_catalog}.{target_schema}.menu")
    .selectExpr("max(recipe_id) as last_recipe")
    .collect()[0]["last_recipe"]
)

try:
    last_dinner = int(
        spark.sql(
            "select max(dinner_id) as last_dinner from"
            f" {target_catalog}.{target_schema}.dinners"
        )
        .collect()[0]
        .asDict()["last_dinner"]
    )
except Exception as e:
    print(f"No last dinner, setting it to 0\n{e}")
    last_dinner = 0

# COMMAND ----------

print(f"last_dinner_id: {last_dinner}")
print(f"target_catalog: {target_catalog}")

# COMMAND ----------

df_dinners = (
    spark.range(((last_dinner + 1) + 1000))
    .withColumnRenamed("id", "dinner_id")
    .withColumn("recipe_id", round(rand() * (available_recipes - 1) + 1, 0))
)

# COMMAND ----------

df_dinners = df_dinners.join(dinner_price, on="recipe_id").withColumn(
    "dinner_date", date_define()
)

# COMMAND ----------

target_table = "dinners"
table_location = f"dbfs:/mnt/demo_storage_account/{target_schema}/{target_table}"
df_dinners.write.mode("append").save(
    table_location
)  # .saveAsTable(f"{target_catalog}.{target_schema}.dinners")

# COMMAND ----------

spark.sql(
    f"CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} LOCATION "
    f" '{table_location}'"
)
