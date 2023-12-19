# Databricks notebook source
# MAGIC %pip install Faker --quiet --disable-pip-version-check
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

from pyspark.sql.types import *

# COMMAND ----------

from faker import Faker

def generate_row() -> dict:
    fake = Faker()
    measurement = {
                "card_holder": fake.name(),
                "card_expiration_date": fake.credit_card_expire(),
                "currency": fake.currency_code(),
                "type": "expense"
            }
    return measurement

generate_row_udf = udf(generate_row, StructType([
    StructField("card_holder", StringType(), False),
    StructField("card_expiration_date", StringType(), False),
    StructField("currency", StringType(), False),
    StructField("type", StringType(), False)]
))
