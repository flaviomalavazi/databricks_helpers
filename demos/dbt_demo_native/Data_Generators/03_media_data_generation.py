# Databricks notebook source
dbutils.widgets.removeAll()
dbutils.widgets.text("target_catalog", "", "Target catalog")
dbutils.widgets.text("target_schema", "", "Target schema")
dbutils.widgets.text("ref_web_events_table", "", "Reference table")
dbutils.widgets.dropdown("reset_data", "false", ["true", "false"], "Reset media data")

target_catalog = dbutils.widgets.get("target_catalog")
target_schema = dbutils.widgets.get("target_schema")
source_table = dbutils.widgets.get("ref_web_events_table")

dbutils.widgets.text("target_table_g", "", "Google table")
dbutils.widgets.text("target_table_f", "", "Facebook table")
dbutils.widgets.text("target_table_b", "", "Bing table")
dbutils.widgets.text("target_table_m", "", "Mailchimp table")

target_table_g = dbutils.widgets.get("target_table_g")
target_table_f = dbutils.widgets.get("target_table_f")
target_table_b = dbutils.widgets.get("target_table_b")
target_table_m = dbutils.widgets.get("target_table_m")
reset_data = True if dbutils.widgets.get("reset_data") == 'true' else False
write_mode = "overwrite" if dbutils.widgets.get("reset_data") == 'true' else "append"

# COMMAND ----------

sources = spark.sql(f"SELECT DISTINCT utm_source FROM {source_table}")

# COMMAND ----------

campaigns = spark.sql(f"SELECT DISTINCT utm_campaign FROM {source_table}")

# COMMAND ----------

ads = spark.sql(f"SELECT DISTINCT utm_content FROM {source_table}")

# COMMAND ----------

dates = spark.sql(f"SELECT DISTINCT date_trunc('hour', event_timestamp) as investment_interval FROM {source_table}")

# COMMAND ----------

visits = spark.sql(f"""SELECT
                            sha2(utm_source || "-" || utm_campaign || "-" || utm_content, 256) as ad_id
                            ,utm_source
                            ,utm_campaign
                            ,utm_content
                            ,DATE_TRUNC('hour', event_timestamp) AS visit_timestamp
                            ,COUNT(DISTINCT event_id) as visits
                        FROM
                            {source_table}
                        WHERE
                           page_url ilike '%?utm_source%' -- counting only the first visits
                        GROUP BY
                            ad_id
                            ,utm_source
                            ,utm_campaign
                            ,utm_content
                            ,visit_timestamp
                    """)

# COMMAND ----------

visits.createOrReplaceTempView("vw_visits")
sources.createOrReplaceTempView("vw_sources")
campaigns.createOrReplaceTempView("vw_campaigns")
ads.createOrReplaceTempView("vw_ads")
dates.createOrReplaceTempView("vw_dates")

# COMMAND ----------

from random import randint, random
generate_investment_lower_band = int(random() * randint(0, 2000))
generate_investment_higher_band = int(randint(generate_investment_lower_band, 5000))

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
df.createOrReplaceTempView("vw_base")

# COMMAND ----------

df = spark.sql(f"""
                SELECT
                    vb.ad_id
                    ,vb.investment_interval
                    ,vb.utm_source
                    ,vb.utm_campaign
                    ,vb.utm_content
                    ,vb.spend
                    ,COALESCE(vv.visits, 0) as visits
                FROM
                    vw_base as vb
                    LEFT JOIN vw_visits as vv on vv.ad_id = vb.ad_id and vv.visit_timestamp = vb.investment_interval
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
                    ,visits
                    ,int(round(visits*(1-(rand()/10))))                             as clicks
                    ,CASE
                        WHEN utm_source = "facebook" THEN 0.0154 + (rand()/300)
                        WHEN utm_source = "google" THEN 0.063 +  (rand()/300)
                        WHEN utm_source = "instagram" THEN 0.0022 + (rand()/300)
                        WHEN utm_source = "bing" THEN 0.0283 + (rand()/300)
                        END                                                         as ctr
                    ,BIGINT(clicks/ctr)                                             as impressions
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
                    ,visits
                    ,int(round(visits*(1-(rand()/10))))                              as emails_clicked
                    ,int(emails_clicked/(100+(rand()*10)))                           as emails_unsubscribed
                    ,int(round(emails_clicked / (0.05+(0.001*int(rand()*10)))))      as emails_opened
                    ,int(round(emails_opened / (0.13+(0.001*int(rand()*10)))))       as emails_delivered
                    ,int(round(emails_delivered/(0.9+(0.01*int(rand()*10)))))        as emails_sent
                    ,int(emails_sent - emails_delivered)                             as emails_bounced
                    ,round((emails_sent/1000)*0.1,2)                                 as emails_cost
                from
                    tab_mailchimp
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
# MAGIC # Writing all media data as managed Delta tables in Unity Catalog

# COMMAND ----------

if reset_data:
    print("Resetting the data")
    spark.sql(f"DROP TABLE IF EXISTS {target_table_g}")
    spark.sql(f"DROP TABLE IF EXISTS {target_table_f}")
    spark.sql(f"DROP TABLE IF EXISTS {target_table_b}")
    spark.sql(f"DROP TABLE IF EXISTS {target_table_m}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the Google ads Delta table

# COMMAND ----------

(
    google.withColumn("last_update_at", lit(datetime.now())).write
        .format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")
        .saveAsTable(target_table_g)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the Facebook ads Delta table

# COMMAND ----------

(
    facebook.withColumn("last_update_at", lit(datetime.now())).write
        .format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")
        .saveAsTable(target_table_f)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the Bing ads Delta table

# COMMAND ----------

(
    bing.withColumn("last_update_at", lit(datetime.now())).write
        .format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")
        .saveAsTable(target_table_b)
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the Mailchimp Delta table

# COMMAND ----------

(
    df_mailchimp.withColumn("last_update_at", lit(datetime.now())).write
        .format("delta")
        .mode(write_mode)
        .option("mergeSchema", "true")
        .saveAsTable(target_table_m)
)

# COMMAND ----------


