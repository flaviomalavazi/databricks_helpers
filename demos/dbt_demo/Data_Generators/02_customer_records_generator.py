# Databricks notebook source
# MAGIC %pip install Faker --quiet --disable-pip-version-check
# MAGIC %pip install mimesis --quiet --disable-pip-version-check
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("ref_bq_table", "bq_federated.flavio_malavazi.tab_web_events", "Reference table")
dbutils.widgets.text("target_table", "flavio_malavazi.dbt_web_events_demo.tab_customer_records", "Target table")

# COMMAND ----------

source_table = dbutils.widgets.get("ref_bq_table")
df = spark.read.table(source_table)

# COMMAND ----------

customers = df.where("page_url_path = '/cart'").select("user_custom_id").drop_duplicates()

# COMMAND ----------

customer_list = [x.user_custom_id for x in customers.select("user_custom_id").collect()]

# COMMAND ----------

from mimesis import Person
from mimesis import Address
from mimesis.enums import Gender
from mimesis import Datetime
person = Person('en')
import pandas as pd
import random
person = Person()
addess = Address()
datetime = Datetime()
def create_rows_mimesis(num=1):
    output = [{"name":person.full_name(gender=Gender.FEMALE),
                   "address":addess.address(),
                   "name":person.name(),
                   "city":addess.city(),
                   "state":addess.state(),
                   "last_update_at":datetime.datetime(),
                   "lucky_number":random.randint(1000,2000)} for x in range(num)]
    return output

df_mimesis = pd.DataFrame(create_rows_mimesis(len(customer_list)))
df_mimesis["email"] = customer_list

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

target_table = dbutils.widgets.get("target_table")

# COMMAND ----------

# If the table doesn't exist, we create it:
if not(spark.catalog.tableExists(target_table)):
    spark.sql(f"CREATE TABLE IF NOT EXISTS {target_table} AS SELECT * FROM updated_records")
else:
    # Else we just merge our changes into it
    spark.sql(f"""
                MERGE INTO {target_table} AS target USING updated_records AS source
                ON target.email = source.email
                WHEN MATCHED THEN UPDATE SET 
                    target.address = source.address,
                    target.name = source.name,
                    target.city = source.city,
                    target.state = source.state,
                    target.last_update_at = source.last_update_at,
                    target.lucky_number = source.lucky_number
                WHEN NOT MATCHED THEN INSERT *;
            """)
