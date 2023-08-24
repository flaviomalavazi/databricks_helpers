# Databricks notebook source
help(dbutils.fs.mount)

# COMMAND ----------

configs = configs = {
    "fs.azure.account.auth.type": "OAuth",
    "fs.azure.account.oauth.provider.type": (
        "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider"
    ),
    "fs.azure.account.oauth2.client.id": CLIENT_ID,
    "fs.azure.account.oauth2.client.secret": CLIENT_SECRET,
    "fs.azure.account.oauth2.client.endpoint": (
        "https://login.microsoftonline.com/TENANT_ID/oauth2/token"
    ),
}

# COMMAND ----------

# Optionally, you can add <directory-name> to the source URI of your mount point.
dbutils.fs.mount(
    source=f"abfss://{CONTAINER}@{STORAGE_ACCOUNT}.dfs.core.windows.net/",
    mount_point="/mnt/demo_storage_account",
    extra_configs=configs,
)

# COMMAND ----------

dbutils.fs.ls("dbfs:/mnt/demo_storage_account/")
