# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("target_catalog", "flavio_malavazi", "Target catalog")
dbutils.widgets.text("target_schema", "dbt_credit_cards_demo_raw", "Target schema")
dbutils.widgets.text("ref_bq_table", "lakehouse_federation_bigquery.flavio_malavazi.tab_web_events", "Reference table")
dbutils.widgets.text("temporary_gcs_bucket", "", "Temp GCS Bucket")
dbutils.widgets.text("target_bucket_path", "", "Target Path")
dbutils.widgets.dropdown("write_mode", "overwrite", ["append", "overwrite"], "Write mode")
dbutils.widgets.dropdown("reset_data", "false", ["true", "false"], "Reset media data")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
target_bucket_path = dbutils.widgets.get("target_bucket_path")
source_table = dbutils.widgets.get("ref_bq_table")
reset_data = True if dbutils.widgets.get("reset_data") == 'true' else False

dbutils.widgets.text("target_bq_table_google", f"{target_catalog}.tab_google_ads", "Target Google table")
dbutils.widgets.text("target_table_f", f"{target_catalog}.{target_schema}.tab_facebook_investment", "Facebook table")
dbutils.widgets.text("target_table_b", f"{target_catalog}.{target_schema}.tab_bing_investment", "Bing table")
dbutils.widgets.text("target_table_m", f"{target_catalog}.{target_schema}.tab_mailchimp_emails", "Mailchimp table")

# COMMAND ----------

sources = spark.sql(f"SELECT DISTINCT utm_source FROM {source_table}")

# COMMAND ----------

campaigns = spark.sql(f"SELECT DISTINCT utm_campaign FROM {source_table}")

# COMMAND ----------

ads = spark.sql(f"SELECT DISTINCT utm_content FROM {source_table}")

# COMMAND ----------

dates = spark.sql(f"SELECT DISTINCT date_trunc('hour', event_timestamp) as investment_interval FROM {source_table}")

# COMMAND ----------

sources.createOrReplaceTempView("vw_sources")
campaigns.createOrReplaceTempView("vw_campaigns")
ads.createOrReplaceTempView("vw_ads")
dates.createOrReplaceTempView("vw_dates")

# COMMAND ----------

from random import randint, random
generate_investment_lower_band = int(random() * randint(0, 2000))
generate_investment_higher_band = int(randint(generate_investment_lower_band, 1000))

# COMMAND ----------

df = spark.sql(f"""
                select
                    sha2(utm_source || "-" || utm_campaign || "-" || utm_content, 256) as ad_id
                    ,investment_interval
                    ,utm_source
                    ,utm_campaign
                    ,utm_content
                    ,case
                        when utm_source = 'organic' then 0
                        else round(round(rand()*({generate_investment_higher_band}-{generate_investment_lower_band})+{generate_investment_lower_band},0)+rand(),2) end as spend
                from
                    vw_sources
                    join vw_campaigns on 1 = 1
                    join vw_ads on 1 = 1
                    join vw_dates on 1 = 1
               """)

# COMMAND ----------

df_media = df.where("utm_source in ('facebook', 'instagram', 'google', 'bing')")
df_media.createOrReplaceTempView("tab_media")
df_mailchimp = df.where("utm_source = 'mailchimp'")
df_mailchimp.createOrReplaceTempView("tab_mailchimp")

# COMMAND ----------

df_media = spark.sql("""
                select
                    ad_id
                    ,investment_interval
                    ,utm_source
                    ,utm_campaign
                    ,utm_content
                    ,spend
                    ,int(round(spend/(rand()/10)))               as impressions
                    ,int(round(impressions*(rand()/10)))         as clicks
                from
                    tab_media
                     """)

# COMMAND ----------

df_mailchimp = spark.sql("""
                select
                    ad_id
                    ,investment_interval
                    ,utm_source
                    ,utm_campaign
                    ,utm_content
                    ,(spend/100)                                                     as emails_cost
                    ,int(round(emails_cost/0.0001))                                  as emails_sent
                    ,int(round(emails_sent * (0.9+(0.01*int(rand()*10)))))           as emails_delivered
                    ,int(emails_sent - emails_delivered)                             as emails_bounced
                    ,int(round(emails_delivered * (0.1+(0.01*int(rand()*10)))))      as emails_opened
                    ,int(round(emails_opened * (0.01+(0.001*int(rand()*10)))))       as emails_clicked
                    ,int(round(emails_opened * (0.0001+(0.00001*int(rand()*10)))))   as emails_unsubscribed
                from
                    tab_media
                     """)

# COMMAND ----------

facebook = df_media.where("utm_source in ('facebook', 'instagram')")
google = df_media.where("utm_source = 'google'")
bing = df_media.where("utm_source = 'bing'")

# COMMAND ----------

from pyspark.sql.functions import lit
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing google data to BQ

# COMMAND ----------

target_bq_table_google = dbutils.widgets.get("target_bq_table_google")
temporary_gcs_bucket = dbutils.widgets.get("temporary_gcs_bucket")
bq_write_mode = dbutils.widgets.get("write_mode")

# COMMAND ----------

(
    google.withColumn("last_update_at", lit(datetime.now())).write
        .format("bigquery")
        .mode(bq_write_mode)
        .option("temporaryGcsBucket", temporary_gcs_bucket)
        .option("table", target_bq_table_google)
        .save()
)

# COMMAND ----------

# MAGIC %md
# MAGIC # Writing the rest of the data as avro tables in Unity Catalog

# COMMAND ----------

if reset_data:
    print("Resetting the data")
    spark.sql(f"drop table {target_catalog}.{target_schema}.tab_facebook_ads")
    dbutils.fs.rm(f"{target_bucket_path}/facebook_ads_data/", True)
    spark.sql(f"drop table {target_catalog}.{target_schema}.tab_bing_ads")
    dbutils.fs.rm(f"{target_bucket_path}/bing_ads_data/", True)
    spark.sql(f"drop table {target_catalog}.{target_schema}.tab_mailchimp")
    dbutils.fs.rm(f"{target_bucket_path}/mailchimp_data/", True)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the Facebook table in Avro

# COMMAND ----------

facebook.withColumn("last_update_at", lit(datetime.now())).write.mode("append").format("com.databricks.spark.avro").save(f"{target_bucket_path}/facebook_ads_data")

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}.tab_facebook_ads
          USING AVRO
          LOCATION '{target_bucket_path}/facebook_ads_data/'
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing bing ads table in Avro

# COMMAND ----------

bing.withColumn("last_update_at", lit(datetime.now())).write.mode("append").format("com.databricks.spark.avro").save(f"{target_bucket_path}/bing_ads_data")

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}.tab_bing_ads
          USING AVRO
          LOCATION '{target_bucket_path}/bing_ads_data/'
          """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing mailchimp table in avro

# COMMAND ----------

df_mailchimp.withColumn("last_update_at", lit(datetime.now())).write.mode("append").format("com.databricks.spark.avro").save(f"{target_bucket_path}/mailchimp_data")

spark.sql(f"""
          CREATE TABLE IF NOT EXISTS {target_catalog}.{target_schema}.tab_mailchimp
          USING AVRO
          LOCATION '{target_bucket_path}/mailchimp_data/'
          """)
