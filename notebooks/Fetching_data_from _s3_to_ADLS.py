# Databricks notebook source
# DBTITLE 1,Setting up s3 client
import boto3

# Setting up the variables:
source_s3_bucket = "SUPPLIER_BUCKET_NAME"

# Creating an S3 client
s3_client = boto3.client('s3',
                            aws_access_key_id = dbutils.secrets.get("DATA_SUPPLIER_SCOPE", "aws_access_key"),
                            aws_secret_access_key = dbutils.secrets.get("DATA_SUPPLIER_SCOPE", "aws_secret_key")
                         )

# COMMAND ----------

# DBTITLE 1,Listing objects in the source S3 (if needed)
# Listing objects already in the source bucket (might not be necessary if we already know the name of the file we want to download)
response = s3_client.list_objects(Bucket=source_s3_bucket, RequestPayer="requester")

# Do something with the list of objects to identify the ones we actually want to download
print(response)

# COMMAND ----------

# DBTITLE 1,Setting up variables for the download operation
s3_object_name = "sample.txt"                                                                               # Object we want to download -> Might include subdirectories from the source S3
YOUR_NAME_FOR_THE_FILE = "my_sample.txt"                                                                    # The name we'll give to the file we're downloading (could match the name of source file without the S3 part)
YOUR_LANDING_PATH_FOR_THE_FILE = "abfss://container@storage_account.dfs.core.windows.net/folder/sub_folder" # ADLS Path
local_download_name = f"/tmp/{YOUR_NAME_FOR_THE_FILE}"                                                      # Local file name as we are going to download the file to the driver and then move it to ADLS for permanent storage
final_path_for_the_file = f"{YOUR_LANDING_PATH_FOR_THE_FILE}/{YOUR_NAME_FOR_THE_FILE}"                      # Path in ADLS longer storage so that we can ingest the file to our DeltaLake

# COMMAND ----------

# DBTITLE 1,Downloading the file from S3 to the Spark Driver
s3_client.download_file(my_bucket, s3_object_name, local_download_name)

# COMMAND ----------

# DBTITLE 1,Moving the file from the local file system to ADLS for long term storage
dbutils.fs.mv(f"file:{local_download_name}", final_path_for_the_file)
