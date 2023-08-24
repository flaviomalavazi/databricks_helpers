# Databricks notebook source
import pyspark.sql.functions as F
from pyspark.sql.types import *

# COMMAND ----------

target_catalog = "hive_metastore"
target_schema = "cloud_migration_demo"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {target_schema}")
spark.sql(f"USE SCHEMA {target_schema}")

# COMMAND ----------

schema = StructType(
    [
        StructField("recipe_id", IntegerType(), False),
        StructField("app", StringType(), False),
        StructField("main", StringType(), False),
        StructField("desert", StringType(), False),
    ]
)

menu_df = spark.createDataFrame(
    [
        (1, "Ceviche", "Tacos", "Flan"),
        (2, "Tomato Soup", "Souffle", "Creme Brulee"),
        (3, "Chips", "Grilled Cheese", "Cheescake"),
        (4, "Green Salad", "Hamburguer", "Chocolate Mousse"),
        (5, "Steak Tartar", "Lasagna", "Ice Cream"),
        (6, "French Fries", "Grilled Pork", "Key Lime Pie"),
        (7, "Bloomin Onion", "Barbequeue Ribs", "Chocolate thunder from down under"),
        (8, "Nuggets", "Big Mac", "Apple pie"),
        (9, "Lobster salad", "Salmon with passion fruit", "Pineaple"),
    ],
    schema,
)

menu_df.createOrReplaceTempView("current_menu")

# COMMAND ----------

target_table = "menu"
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (recipe_id INT, app STRING, main STRING, desert STRING)
    USING DELTA
    LOCATION 'dbfs:/mnt/demo_storage_account/{target_schema}/{target_table}'"""
)

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO cloud_migration_demo.menu as target USING current_menu as source
# MAGIC   ON target.recipe_id = source.recipe_id
# MAGIC   WHEN MATCHED THEN UPDATE SET *
# MAGIC   WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

target_table = "dinner"
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (recipe_id INT, full_menu STRING)
    USING DELTA
    LOCATION 'dbfs:/mnt/demo_storage_account/{target_schema}/{target_table}'"""
)

# COMMAND ----------

spark.sql(
    f"""MERGE INTO {target_schema}.{target_table} as target USING (SELECT recipe_id, concat(app," + ", main," + ",desert) as full_menu FROM {target_schema}.menu) as source
  ON target.recipe_id = source.recipe_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;"""
)

# COMMAND ----------

last_id = (
    spark.read.table(f"{target_schema}.dinner")
    .select(F.max("recipe_id").alias("last_recipe"))
    .collect()[0]["last_recipe"]
)
print(f"last_id = {last_id}")

# COMMAND ----------

df = (
    spark.range(last_id)
    .withColumn("price", F.round(10 * F.rand(seed=42), 2))
    .withColumnRenamed("id", "recipe_id")
    .withColumn("recipe_id", F.col("recipe_id") + F.lit(1))
)
target_table = "price"
df.write.mode("overwrite").save(
    f"dbfs:/mnt/demo_storage_account/{target_schema}/{target_table}"
)
spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} LOCATION 'dbfs:/mnt/demo_storage_account/{target_schema}/{target_table}'"""
)

dinner = spark.read.table(f"{target_schema}.dinner")
price = spark.read.table(f"{target_schema}.price")

dinner_price = dinner.join(price, on="recipe_id")
dinner_price.createOrReplaceTempView("current_dinner_prices")

# COMMAND ----------

target_table = "dinner_price"

spark.sql(
    f"""CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (recipe_id INT, full_menu STRING, price DOUBLE)
    USING DELTA
    LOCATION 'dbfs:/mnt/demo_storage_account/{target_schema}/{target_table}'"""
)

spark.sql(
    f"""MERGE INTO {target_schema}.{target_table} as target USING current_dinner_prices as source
  ON target.recipe_id = source.recipe_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;"""
)

# COMMAND ----------

# DBTITLE 1,Criando uma tabela em outro schema
target_schema = "second_cloud_migration_schema"
target_table = "tab_new_menu"

schema = StructType(
    [
        StructField("recipe_id", IntegerType(), False),
        StructField("app", StringType(), False),
        StructField("main", StringType(), False),
        StructField("desert", StringType(), False),
    ]
)

menu_df = spark.createDataFrame(
    [
        (1, "Ceviche", "Tacos", "Flan"),
        (2, "Tomato Soup", "Souffle", "Creme Brulee"),
        (3, "Chips", "Grilled Cheese", "Cheescake"),
        (4, "Green Salad", "Hamburguer", "Chocolate Mousse"),
        (5, "Steak Tartar", "Lasagna", "Ice Cream"),
        (6, "French Fries", "Grilled Pork", "Key Lime Pie"),
        (7, "Bloomin Onion", "Barbequeue Ribs", "Chocolate thunder from down under"),
        (8, "Nuggets", "Big Mac", "Apple pie"),
        (9, "Lobster salad", "Salmon with passion fruit", "Pineaple"),
    ],
    schema,
)

menu_df.createOrReplaceTempView("new_menu")

spark.sql(f"""CREATE SCHEMA IF NOT EXISTS {target_schema};""")

spark.sql(
    f"""
    CREATE TABLE IF NOT EXISTS {target_schema}.{target_table} (recipe_id INT, app STRING, main STRING, desert STRING)
    USING DELTA
    LOCATION 'dbfs:/mnt/demo_storage_account/{target_schema}/{target_table}';"""
)

spark.sql(
    f"""MERGE INTO {target_schema}.{target_table} as target USING new_menu as source
  ON target.recipe_id = source.recipe_id
  WHEN MATCHED THEN UPDATE SET *
  WHEN NOT MATCHED THEN INSERT *;
  """
)
