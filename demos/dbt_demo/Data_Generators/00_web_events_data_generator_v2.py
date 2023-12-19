# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("temporary_gcs_bucket", "", "Temp GCS Bucket")
dbutils.widgets.text("target_bq_table", "", "Target bq table")
dbutils.widgets.dropdown("reset_data", "false", ["false", "true"], "Reset data")
dbutils.widgets.text("number_of_users", "1000", "Number of random users")
dbutils.widgets.text("sessions_per_day", "30000", "Number of sessions per day")
dbutils.widgets.text("simulation_duration", "120", "Simulation duration")

# COMMAND ----------

temporary_gcs_bucket = dbutils.widgets.get("temporary_gcs_bucket")
target_bq_table = dbutils.widgets.get("target_bq_table")
write_mode = "overwrite" if dbutils.widgets.get("reset_data") == "true" else "append"
number_of_users = int(dbutils.widgets.get("number_of_users"))
sessions_per_day = int(dbutils.widgets.get("sessions_per_day"))
simulation_duration =int(dbutils.widgets.get("simulation_duration"))

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark.sql.window import Window
from typing import List

# COMMAND ----------

from datetime import datetime, timedelta
from random import randrange, choice
spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", True)

def string_generation(list_to_convert: List[str]) -> str:
    return "'"+"','".join(list_to_convert)+"'"

def generate_random_timestamp() -> str:
    session_distribution = [
    0, 0, 0, 0, 
    1, 1, 1,
    2, 2,
    3, 3,
    4, 4,
    5, 5,
    6, 6, 6,
    7, 7, 7, 7,
    8, 8, 8, 8, 8,
    9, 9, 9, 9, 9, 9,
    10,10,10,10,10,10,10,
    11,11,11,11,11,11,11,11,
    12,12,12,12,12,12,12,12,12,12,
    13,13,13,13,13,13,13,13,13,
    14,14,14,14,14,14,14,
    15,15,15,15,15,15,
    16,16,16,16,16,
    17,17,17,17,17,17,17,
    18,18,18,18,18,18,18,18,18,
    19,19,19,19,19,19,19,19,19,19,19,
    20,20,20,20,20,20,20,20,20,20,20,20,
    21,21,21,21,21,21,21,21,21,21,21,21,21,21,
    22,22,22,22,22,22,
    23,23,23,23,23
    ]
    timestamp = datetime.today() + timedelta(days = randrange(-simulation_duration,0), hours=choice(session_distribution), minutes= randrange(0,59))
    return datetime.strftime(timestamp, "%Y-%m-%d %H:%M:%S.%f")

generate_random_timestamp_udf = udf(generate_random_timestamp, StringType())

BROWSERS = (
    ["chrome"]*70 + 
    ["safari"]*9 + 
    ["edge"]*20 + 
    ["opera"]*1
)

ENTRY_PAGES = (
    ["https://my-awesome-website.com/"]*5 + 
    ["https://my-awesome-website.com/products/product_a/"]*3 + 
    ["https://my-awesome-website.com/products/product_b/"]*1 + 
    ["https://my-awesome-website.com/marketplace/"]*1
)

LEAD_GENERATION_CHANNELS = (
    ["utm_source=organic"]*10 +
    ["utm_source=google"]*5 + 
    ["utm_source=facebook"]*2 + 
    ["utm_source=instagram"]*4 + 
    ["utm_source=bing"]*1 + 
    ["utm_source=mailchimp"]*4
)

LEAD_GENERATION_CAMPAIGNS = (
    ["utm_campaign=campaign_1"]*10 +
    ["utm_campaign=campaign_2"]*5 + 
    ["utm_campaign=campaign_3"]*3 + 
    ["utm_campaign=campaign_4"]*2
)

LEAD_GENERATION_ADS = (
    ["utm_content=ad_1"]*2 +
    ["utm_content=ad_2"]*3 + 
    ["utm_content=ad_3"]*10 + 
    ["utm_content=ad_4"]*5
)

BROWSER_LIST = string_generation(BROWSERS)
ENTRY_PAGE_LIST = string_generation(ENTRY_PAGES)
LEAD_GENERATION_CHANNEL_LIST = string_generation(LEAD_GENERATION_CHANNELS)
LEAD_GENERATION_CAMPAIGN_LIST = string_generation(LEAD_GENERATION_CAMPAIGNS)
LEAD_GENERATION_AD_LIST = string_generation(LEAD_GENERATION_ADS)

# COMMAND ----------

df_sessions = (
    (
    spark.range(number_of_users)
    .withColumn("user_id", F.expr("uuid()"))
    .sample(True, ((sessions_per_day*simulation_duration)/number_of_users))
    .select("user_id")
    .withColumn("session_uuid", F.expr("uuid()"))
    .withColumn("session_start", generate_random_timestamp_udf())
    ).selectExpr(
        "session_uuid",
        "session_start",
        "user_id",
        f"array({BROWSER_LIST})[int(rand()*{len(BROWSERS)})] as browser",
        f"array({ENTRY_PAGE_LIST})[int(rand()*{len(ENTRY_PAGES)})] as page_url",
        f"array({LEAD_GENERATION_CHANNEL_LIST})[int(rand()*{len(LEAD_GENERATION_CHANNELS)})] as utm_source",
        f"array({LEAD_GENERATION_CAMPAIGN_LIST})[int(rand()*{len(LEAD_GENERATION_CAMPAIGNS)})] as utm_campaign",
        f"array({LEAD_GENERATION_AD_LIST})[int(rand()*{len(LEAD_GENERATION_ADS)})] as utm_content",
    )
    .withColumn("base_progression_probability", F.rand())
    .withColumn("page_url_domain", F.concat(F.split_part("page_url", F.lit(".com/"), F.lit(1)), F.lit(".com/")))
    .withColumn("page_url_path", F.coalesce(F.nullif(F.split_part("page_url", F.lit(".com/"), F.lit(2)), F.lit("")), F.lit("/")))
    .withColumn("page_url", F.concat("page_url", F.lit("?"), "utm_source", F.lit("&"), "utm_campaign", F.lit("&"), "utm_content", F.lit("&")))
    .withColumn("utm_source", F.coalesce(F.nullif(F.split_part("utm_source", F.lit("utm_source="), F.lit(2)), F.lit("")), F.lit("organic")))
    .withColumn("utm_campaign", F.coalesce(F.nullif(F.split_part("utm_campaign", F.lit("utm_campaign="), F.lit(2)), F.lit("")), F.lit("organic")))
    .withColumn("utm_content", F.coalesce(F.nullif(F.split_part("utm_content", F.lit("utm_content="), F.lit(2)), F.lit("")), F.lit("organic")))
    .selectExpr("*",
                """
                CASE
                    WHEN page_url_path = "/" and utm_source = 'organic' THEN (base_progression_probability * 0.6)
                    WHEN page_url_path = "/" and utm_source = 'google' THEN (base_progression_probability * 0.4)
                    WHEN page_url_path = "/" and utm_source in ('facebook', 'instagram') THEN (base_progression_probability * 0.2)
                    WHEN page_url_path = "/" and utm_source = 'bing' THEN (base_progression_probability * 0.4)
                    WHEN page_url_path = "/" and utm_source = 'mailchimp' THEN (base_progression_probability * 0.5)
                    WHEN page_url_path = "products/product_a/" and utm_source = 'organic' THEN (base_progression_probability * 0.75)
                    WHEN page_url_path = "products/product_a/" and utm_source = 'google' THEN (base_progression_probability * 0.55)
                    WHEN page_url_path = "products/product_a/" and utm_source in ('facebook', 'instagram') THEN (base_progression_probability * 0.35)
                    WHEN page_url_path = "products/product_a/" and utm_source = 'bing' THEN (base_progression_probability * 0.45)
                    WHEN page_url_path = "products/product_a/" and utm_source = 'mailchimp' THEN (base_progression_probability * 0.65)
                    WHEN page_url_path = "products/product_b/" and utm_source = 'organic' THEN (base_progression_probability * 0.75)
                    WHEN page_url_path = "products/product_b/" and utm_source = 'google' THEN (base_progression_probability * 0.55)
                    WHEN page_url_path = "products/product_b/" and utm_source in ('facebook', 'instagram') THEN (base_progression_probability * 0.35)
                    WHEN page_url_path = "products/product_b/" and utm_source = 'bing' THEN (base_progression_probability * 0.45)
                    WHEN page_url_path = "products/product_b/" and utm_source = 'mailchimp' THEN (base_progression_probability * 0.65)
                    WHEN page_url_path = "marketplace/" and utm_source = 'organic' THEN (base_progression_probability * 0.7)
                    WHEN page_url_path = "marketplace/" and utm_source = 'google' THEN (base_progression_probability * 0.5)
                    WHEN page_url_path = "marketplace/" and utm_source in ('facebook', 'instagram') THEN (base_progression_probability * 0.3)
                    WHEN page_url_path = "marketplace/" and utm_source = 'bing' THEN (base_progression_probability * 0.5)
                    WHEN page_url_path = "marketplace/" and utm_source = 'mailchimp' THEN (base_progression_probability * 0.6)
                    ELSE rand()
                    END as progression_probability
                """
                )
    .selectExpr("*",
                """
                CASE
                    WHEN progression_probability <= 0.14982 THEN 1
                    WHEN progression_probability <= 0.29962 THEN 2
                    WHEN progression_probability <= 0.44942 THEN 3
                    WHEN progression_probability <= 0.59922 and page_url_path in ("/", "marketplace/") THEN 4
                    WHEN progression_probability <= 0.59922 and page_url_path in ("products/product_b/", "products/product_a/") THEN 3
                    WHEN progression_probability > 0.59922 and page_url_path in ("/", "marketplace/") THEN 5
                    WHEN progression_probability > 0.59922 and page_url_path in ("products/product_b/", "products/product_a/") THEN 4
                    ELSE 1
                    END as number_of_events
                """
                )
    .withColumn("event_identifier", F.expr("explode(array_repeat(number_of_events,int(number_of_events)))"))
    .withColumn("page_order", F.row_number().over(Window.partitionBy("session_uuid").orderBy(F.lit(F.rand()))))
    .selectExpr("*",
                """CASE
                    -- The first page is always the first page
                    WHEN page_order = 1 THEN page_url
                    -- Customer has reached our website and will see one more page
                    WHEN number_of_events = 2 and page_order = 2 and page_url_path in ("marketplace/", "/") AND progression_probability <= 0.37 THEN page_url_domain || 'product_a/'
                    WHEN number_of_events = 2 and page_order = 2 and page_url_path in ("marketplace/", "/") AND progression_probability > 0.37 THEN page_url_domain || 'product_b/'
                    WHEN number_of_events = 2 and page_order = 2 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'cart/'
                    -- Customer will visit three pages
                    WHEN number_of_events = 3 and page_order = 2 and page_url_path in ("marketplace/", "/") AND progression_probability <= 0.37 THEN page_url_domain || 'product_a/'
                    WHEN number_of_events = 3 and page_order = 2 and page_url_path in ("marketplace/", "/") AND progression_probability > 0.37 THEN page_url_domain || 'product_b/'
                    WHEN number_of_events = 3 and page_order = 2 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'cart/'
                    WHEN number_of_events = 3 and page_order = 3 and page_url_path in ("marketplace/", "/") THEN page_url_domain || "cart/"
                    WHEN number_of_events = 3 and page_order = 3 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'payment/'
                    -- Customer will visit four pages
                    WHEN number_of_events = 4 and page_order = 2 and page_url_path in ("marketplace/", "/") AND progression_probability <= 0.37 THEN page_url_domain || 'product_a/'
                    WHEN number_of_events = 4 and page_order = 2 and page_url_path in ("marketplace/", "/") AND progression_probability > 0.37 THEN page_url_domain || 'product_b/'
                    WHEN number_of_events = 4 and page_order = 2 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'cart/'
                    WHEN number_of_events = 4 and page_order = 3 and page_url_path in ("marketplace/", "/") THEN page_url_domain || "cart/"
                    WHEN number_of_events = 4 and page_order = 3 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'payment/'
                    WHEN number_of_events = 4 and page_order = 4 and page_url_path in ("marketplace/", "/") THEN page_url_domain || "payment/"
                    WHEN number_of_events = 4 and page_order = 4 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'confirmation/'
                    -- Customer will visit five pages
                    WHEN number_of_events = 5 and page_order = 2 and page_url_path in ("marketplace/", "/") AND progression_probability <= 0.37 THEN page_url_domain || 'product_a/'
                    WHEN number_of_events = 5 and page_order = 2 and page_url_path in ("marketplace/", "/") AND progression_probability > 0.37 THEN page_url_domain || 'product_b/'
                    WHEN number_of_events = 5 and page_order = 2 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'cart/'
                    WHEN number_of_events = 5 and page_order = 3 and page_url_path in ("marketplace/", "/") THEN page_url_domain || "cart/"
                    WHEN number_of_events = 5 and page_order = 3 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'payment/'
                    WHEN number_of_events = 5 and page_order = 4 and page_url_path in ("marketplace/", "/") THEN page_url_domain || "payment/"
                    WHEN number_of_events = 5 and page_order = 4 and page_url_path in ("products/product_a/", "products/product_b/") THEN page_url_domain || 'confirmation/'
                    WHEN number_of_events = 5 and page_order = 5 and page_url_path in ("marketplace/", "/") THEN page_url_domain || "confirmation/"
                    END as new_page_url
                """
                )
    .drop("page_url")
    .withColumnRenamed("new_page_url", "page_url")
    .withColumn("session_start", F.expr(f"timestampadd(SECOND, rand()*30*page_order, now())"))
    .withColumnRenamed("session_start", "event_timestamp")
    .withColumn("event_id", F.expr("uuid()"))
    .withColumn("page_url_domain", F.concat(F.split_part("page_url", F.lit(".com/"), F.lit(1)), F.lit(".com/")))
    .withColumn("page_url_path", F.coalesce(F.nullif(F.split_part("page_url", F.lit(".com/"), F.lit(2)), F.lit("")), F.lit("/")))
    .withColumn("page_url_path", F.coalesce(F.nullif(F.split_part("page_url", F.lit("?"), F.lit(1)), F.lit("")), "page_url_path"))
    .withColumn("page_url_path", F.coalesce(F.nullif(F.split_part("page_url_path", F.lit(".com/"), F.lit(2)), F.lit("")), F.lit("/")))
    .drop("base_progression_probability", "number_of_events", "event_identifier", "page_order", "progression_probability")
)

# COMMAND ----------

print(f"Writing to BigQuery...\t\t{datetime.now()}")
(
    df_sessions.write
        .format("bigquery")
        .mode(write_mode)
        .option("temporaryGcsBucket", temporary_gcs_bucket)
        .option("table", target_bq_table)
        .save()
)
print(f"All Done!\t\t\t{datetime.now()}")
