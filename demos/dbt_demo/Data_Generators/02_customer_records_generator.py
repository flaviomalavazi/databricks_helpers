# Databricks notebook source
# MAGIC %pip install Faker --quiet --disable-pip-version-check
# MAGIC %pip install mimesis --quiet --disable-pip-version-check
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("target_catalog", "", "Target catalog")
dbutils.widgets.text("target_schema", "", "Target schema")
dbutils.widgets.text("ref_bq_table", "", "Reference table")
dbutils.widgets.dropdown("reset_data", "false", ["true", "false"], "Reset the data")
dbutils.widgets.text("target_table", "", "Target table")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_table = dbutils.widgets.get("target_table")
source_table = dbutils.widgets.get("ref_bq_table")
reset_data = True if dbutils.widgets.get("reset_data") == 'true' else False


# COMMAND ----------

if reset_data:
    print("Resetting table payments data")
    spark.sql(f"DROP TABLE IF EXISTS {target_table}")

# COMMAND ----------

df = spark.read.table(source_table)

# COMMAND ----------

customers = df.where("page_url_path = 'cart/'").select("user_id").drop_duplicates()

# COMMAND ----------

customer_list = [x.user_id for x in customers.select("user_id").collect()]

# COMMAND ----------

from mimesis import Person
from mimesis import Address
from mimesis import Datetime
import pandas as pd
import numpy as np
import random
person = Person('en')
person = Person()
addess = Address()
datetime = Datetime()
EMAIL_PROVIDERS = (
    ["@gmail.com"]*140 + 
    ["@hotmail.com"]*20 +
    ["@outlook.com"]*18 + 
    ["@icloud.com"]* 10 +
    ["@uol.com.br"]*1 + 
    ["@bol.com.br"]*1
)
def create_rows_mimesis(num=1):
    output = [{"name":person.full_name(),
                   "address":addess.address(),
                   "username": person.username(),
                   "name":person.name(),
                   "city":addess.city(),
                   "state":addess.state(),
                   "last_update_at":datetime.datetime(),
                   "lucky_number":random.randint(0,2000)} for x in range(num)]
    return output



email_providers = list(np.random.choice(EMAIL_PROVIDERS, len(customer_list)))
df_mimesis = pd.DataFrame(create_rows_mimesis(len(customer_list)))
df_mimesis["email_id"] = customer_list
df_mimesis["email_provider"] = email_providers

# COMMAND ----------

from datetime import datetime
from pyspark.sql.functions import lit
# Enable Arrow-based columnar data transfers
spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
# Create a Spark DataFrame from a pandas DataFrame using Arrow
df = spark.createDataFrame(df_mimesis)
df = df.withColumn("record_created_at", lit(datetime.now()))
df.createOrReplaceTempView("updated_records")

# COMMAND ----------

# If the table doesn't exist, we create it:
if not(spark.catalog.tableExists(target_table)):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM updated_records")
else:
    # Else we just merge our changes into it
    spark.sql(f"""
                MERGE INTO {target_table} AS target USING updated_records AS source
                ON target.email_id = source.email_id
                WHEN MATCHED THEN UPDATE SET 
                    target.address = source.address,
                    target.name = source.name,
                    target.username = source.username,
                    target.email_provider = source.email_provider,
                    target.city = source.city,
                    target.state = source.state,
                    target.last_update_at = source.last_update_at,
                    target.lucky_number = source.lucky_number
                WHEN NOT MATCHED THEN INSERT *;
            """)
