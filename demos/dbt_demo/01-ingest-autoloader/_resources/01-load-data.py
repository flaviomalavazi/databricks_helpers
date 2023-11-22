# Databricks notebook source
# MAGIC %md
# MAGIC # Data initialization notebook. 
# MAGIC Do not run outside of the main notebook. This will automatically be called based on the reste_all widget value to setup the data required for the demo.

# COMMAND ----------

# MAGIC %pip install Faker
# MAGIC %pip install dbldatagen
# MAGIC dbutils.library.restartPython()

# COMMAND ----------

#dbutils.widgets.text("raw_data_location", "/demos/retail/churn/", "Raw data location (stating dir)")
dbutils.widgets.dropdown("reset_all_data", "false", ["true", "false"], "Reset all data")
dbutils.widgets.text("target_catalog", "main", "Target Catalog")
dbutils.widgets.text("target_schema", "default", "Target Schema")
dbutils.widgets.text("source_volume", "dbt_demo", "Source Volume")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
source_volume = dbutils.widgets.get("source_volume")
reset_all_data = dbutils.widgets.get("reset_all_data") == "true"

print("01-load-data notebook widget values:")
print(f"reset_all_data = {reset_all_data}")
print(f"target_catalog = {target_catalog}")
print(f"target_schema = {target_schema}")
print(f"source_volume = {source_volume}")

def cleanup_folder(path):
  #Cleanup to have something nicer
  for f in dbutils.fs.ls(path):
    if f.name.startswith('_committed') or f.name.startswith('_started') or f.name.startswith('_SUCCESS') :
      dbutils.fs.rm(f.path)

# COMMAND ----------

folder = f"/Volumes/{target_catalog}/{target_schema}/{source_volume}/dbdemos/dbt-retail"
if reset_all_data:
  print("resetting all data...")
  if folder.count('/') > 2:
    dbutils.fs.rm(folder, True)

data_exists = False
try:
  dbutils.fs.ls(folder)
  dbutils.fs.ls(folder+"/orders")
  dbutils.fs.ls(folder+"/users")
  dbutils.fs.ls(folder+"/events")
  generate_data = True
  print("data already exists")
except:
  print("folder doesn't exists, generating the data...")

if data_exists:
  dbutils.notebook.exit("data already exists")

# COMMAND ----------

# DBTITLE 1,users data
from pyspark.sql import functions as F
from faker import Faker
import pandas as pd
from collections import OrderedDict 
import multiprocessing
import uuid
fake = Faker()
import random
from datetime import datetime, timedelta

def generate_bulk(bulk_size):
    fake = Faker()

    fake_data = [generate_user_record(fake) for i in range(bulk_size)]

    return fake_data

def generate_user_record(fake_generator: fake, months=36):
  return {
  "firstname": fake.first_name(),
  "lastname": fake.last_name(),
  "id": str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None,
  "email": fake.ascii_company_email(),
  "address": fake.address(),
  "canal": fake.random_elements(elements=OrderedDict([("WEBAPP", 0.5),("MOBILE", 0.1),("PHONE", 0.3),(None, 0.01)]), length=1)[0],
  "country": random.choice(['FR', 'USA', 'SPAIN']),
  "creation_date": fake.date_between_dates(date_start=(datetime.now() - timedelta(days=30*months)), date_end=(datetime.now() - timedelta(days=30*months)) + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S"),
  "last_activity_date": fake.date_time_this_month().strftime("%m-%d-%Y %H:%M:%S"),
  "gender": random.choice([0,1,2]),
  "age_group": random.randint(0,9),
  }

pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)

data = pool.map(generate_bulk, [10000])[0]
ids = [r["id"] for r in data]
df_customers = spark.createDataFrame(data=data)

# COMMAND ----------

#Number of order per customer to generate a nicely distributed dataset
import numpy as np
np.random.seed(0)
mu, sigma = 3, 2 # mean and standard deviation
s = np.random.normal(mu, sigma, int(len(ids)))
s = [i if i > 0 else 0 for i in s]

#Most of our customers have ~3 orders
import matplotlib.pyplot as plt
count, bins, ignored = plt.hist(s, 30, density=False)
plt.show()
s = [int(i) for i in s]

order_user_ids = list()
action_user_ids = list()
for i, id in enumerate(ids):
  for j in range(1, s[i]):
    order_user_ids.append(id)
    #Let's make 5 more actions per order (5 click on the website to buy something)
    for j in range(1, 5):
      action_user_ids.append(id)
      
print(f"Generated {len(order_user_ids)} orders and  {len(action_user_ids)} actions for {len(ids)} users")

# COMMAND ----------

# DBTITLE 1,order data
def generate_bulk_orders(bulk_size):
    fake = Faker()
    fake_data = [generate_orders(fake) for i in range(bulk_size)]
    return fake_data

def generate_orders(fake_generator: fake, months=36):
    item_number = random.randint(1,50)
    return {
        "id": str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None,
        "transaction_date": fake.date_between_dates(date_start=(datetime.now() - timedelta(days=30*months)), date_end=(datetime.now() - timedelta(days=30*months)) + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S"),
        "item_count": item_number,
        "amount": item_number*(random.random()*random.randint(1,5)),
    }

pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)

data = pool.map(generate_bulk_orders, [len(order_user_ids)])[0]
df_orders = pd.DataFrame(data)
df_orders["user_id"] = order_user_ids
orders = spark.createDataFrame(data=df_orders)
orders.repartition(10).write.format("json").mode("overwrite").save(folder+"/orders")
cleanup_folder(folder+"/orders")

# COMMAND ----------

# DBTITLE 1,website actions
#Website interaction
from datetime import timedelta, datetime

from pyspark.sql.functions import count, when, isnan, col, lit, countDistinct
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, TimestampType

import dbldatagen as dg

def generate_interactions(fake_generator: fake, months=36):
    return {
        "event_id": str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None,
        "date": fake.date_between_dates(date_start=(datetime.now() - timedelta(days=30*months)), date_end=(datetime.now() - timedelta(days=30*months)) + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S"),
        "platform": lambda:fake.random_elements(elements=OrderedDict([("ios", 0.5),("android", 0.1),("other", 0.3),(None, 0.01)]), length=1)[0],
        "action": lambda:fake.random_elements(elements=OrderedDict([("view", 0.5),("log", 0.1),("click", 0.3),(None, 0.01)]), length=1)[0],
        # "url": lambda:re.sub(r'https?:\/\/.*?\/', "https://databricks.com/", fake.uri()),
        "session_id": str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None,
    }

pool = multiprocessing.Pool(processes=multiprocessing.cpu_count() - 1)

data = pool.map(generate_bulk_interactions, [len(action_user_ids)])[0]
df_actions = pd.DataFrame(data)
df_actions["user_id"] = action_user_ids

# COMMAND ----------

fake.uri()

# COMMAND ----------

interval = timedelta(days=1, hours=1)
start = datetime.now()
end = datetime.now() - timedelta(days=30*36)
devices = ["ios"]*50 + ["android"]*10 + ["other"]*30 + [None]*1
actions = ["view"]*50 + ["log"]*10 + ["click"]*30 + [None]*1

schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("date", TimestampType(), True),
    StructField("platform", StringType(), True),
    StructField("action", StringType(), True),
    StructField("url", StringType(), True),
    StructField("session_id", StringType(), True),
])

#         "event_id": str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None,
#         "date": fake.date_between_dates(date_start=(datetime.now() - timedelta(days=30*months)), date_end=(datetime.now() - timedelta(days=30*months)) + timedelta(days=30)).strftime("%m-%d-%Y %H:%M:%S"),
#         "platform": lambda:fake.random_elements(elements=OrderedDict([("ios", 0.5),("android", 0.1),("other", 0.3),(None, 0.01)]), length=1)[0],
#         "action": lambda:fake.random_elements(elements=OrderedDict([("view", 0.5),("log", 0.1),("click", 0.3),(None, 0.01)]), length=1)[0],
#         "url": lambda:re.sub(r'https?:\/\/.*?\/', "https://databricks.com/", fake.uri()),

#         "session_id": str(uuid.uuid4()) if random.uniform(0, 1) < 0.98 else None,
# will have implied column `id` for ordinal of row
data = (dg.DataGenerator(sparkSession=spark, name="creating_actions_dataset", rows=len(action_user_ids), partitions=20)
      .withSchema(schema)
      # withColumnSpec adds specification for existing column
      .withColumnSpec("event_id", expr="CASE WHEN rand() < 0.98 THEN uuid() ELSE NULL END")
      .withColumnSpec("date", "timestamp", begin=start, end=end, interval=interval, random=True)
      .withColumnSpec("platform", values=devices, random=True)
      .withColumnSpec("action", values=actions, random=True)
      .withColumnSpec("session_id", expr="CASE WHEN rand() < 0.98 THEN uuid() ELSE NULL END")
      )

actions_df = data.build(withTempView=True)

print(data.schema)
actions_df.printSchema()
# display(x3_output)


# COMMAND ----------



# COMMAND ----------

actions = spark.createDataFrame(data=df_actions)
actions.write.format("csv").option("header", True).mode("overwrite").save(folder+"/events")
cleanup_folder(folder+"/events")

# COMMAND ----------

# DBTITLE 1,Compute churn and save users
#Let's generate the Churn information. We'll fake it based on the existing data & let our ML model learn it
from pyspark.sql.functions import col
import pyspark.sql.functions as F

churn_proba_action = actions.groupBy('user_id').agg({'platform': 'first', '*': 'count'}).withColumnRenamed("count(1)", "action_count")
#Let's count how many order we have per customer.
churn_proba = orders.groupBy('user_id').agg({'item_count': 'sum', '*': 'count'})
churn_proba = churn_proba.join(churn_proba_action, ['user_id'])
churn_proba = churn_proba.join(df_customers, churn_proba.user_id == df_customers.id)

#Customer having > 5 orders are likely to churn

churn_proba = (churn_proba.withColumn("churn_proba", 5 +  F.when(((col("count(1)") >=5) & (col("first(platform)") == "ios")) |
                                                                 ((col("count(1)") ==3) & (col("gender") == 0)) |
                                                                 ((col("count(1)") ==2) & (col("gender") == 1) & (col("age_group") <= 3)) |
                                                                 ((col("sum(item_count)") <=1) & (col("first(platform)") == "android")) |
                                                                 ((col("sum(item_count)") >=10) & (col("first(platform)") == "ios")) |
                                                                 (col("action_count") >=4) |
                                                                 (col("country") == "USA") |
                                                                 ((F.datediff(F.current_timestamp(), col("creation_date")) >= 90)) |
                                                                 ((col("age_group") >= 7) & (col("gender") == 0)) |
                                                                 ((col("age_group") <= 2) & (col("gender") == 1)), 80).otherwise(20)))

churn_proba = churn_proba.withColumn("churn", F.rand()*100 < col("churn_proba"))
churn_proba = churn_proba.drop("user_id", "churn_proba", "sum(item_count)", "count(1)", "first(platform)", "action_count")
churn_proba.repartition(100).write.format("parquet").mode("overwrite").save(folder+"/users")
cleanup_folder(folder+"/users")
