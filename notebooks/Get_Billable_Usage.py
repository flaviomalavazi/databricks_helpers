# Databricks notebook source
import base64
import requests
import json
import io
import pandas as pd
from datetime import datetime
from pyspark.sql import DataFrame

class databricksAccountHandler():
    def __init__(self, account_id: str, username: str, password: str):
        authentication = base64.b64encode(f"{username}:{password}".encode('ascii')).decode('ascii')
        self.headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Basic {authentication}'
        }
        self.base_url = f"https://accounts.cloud.databricks.com/api/2.0/accounts/{account_id}"

    def fetch_usage(self, start_month: datetime, end_month: datetime, personal_data: bool = False, temp_path: str = "", temp_file_name: str = "", file_format: str = "csv", clean_temp_file: bool = True) -> DataFrame:
        temp_path = temp_path if temp_path != "" else f"dbfs:/Users/{spark.sql('select current_user() as me').collect()[0]['me']}/temp_files/"
        if temp_path.startswith("/dbfs/"):
            temp_path = temp_path.replace("/dbfs/", "dbfs:/")
        self.__check_temp_path_existence(temp_path)
        file_name = f"{datetime.strftime(datetime.now(), '%Y%m%dT%H%M%S')}_{temp_file_name}.{document_format}" if temp_file_name != "" else f"{datetime.strftime(datetime.now(), '%Y%m%dT%H%M%S')}_content.{file_format}"
        save_path = f"{temp_path}{file_name}"
        response = self.__get_billable_usage(start_month = start_month, end_month=end_month, personal_data=personal_data)
        if self.__save_api_response_to_temp_file(download = response, save_path = save_path):
            df = self.__read_temp_file(save_path = save_path, document_format = file_format, options={"header": "true"})
            df.cache()
            print = f"{df.count()}"
        if clean_temp_file:
            dbutils.fs.rm(f"{save_path}")
        return df

    def __get_billable_usage(self, start_month: datetime, end_month: datetime, personal_data: bool = False) -> requests.Response:
        personal_data = "true" if personal_data else "false"
        start_month = datetime.strftime(start_month, "%Y-%m")
        end_month = datetime.strftime(end_month, "%Y-%m")
        url = f"{self.base_url}/usage/download?start_month={start_month}&end_month={end_month}&personal_data={personal_data}"
        download = requests.request("GET", url, headers=self.headers, data={})
        return download

    def __check_temp_path_existence(self, temp_path: str) -> None:
        try:
            dbutils.fs.ls(temp_path)
        except Exception as e:
            if "java.io.FileNotFoundException" in str(e):
                dbutils.fs.mkdirs(temp_path)
    
    def __save_api_response_to_temp_file(self, download: requests.Response, save_path: str) -> bool:
        if download.status_code == 200:
            dbutils.fs.put(save_path, download.content.decode('utf-8'), True)
            return True
        else:
            return False

    def __read_temp_file(self, save_path: str, document_format: str, options: dict) -> DataFrame:
        try:
            df = spark.read.options(**options).format(document_format).load(save_path)
            return df
        except Exception as e:
            raise(e)


# COMMAND ----------

from pandas import offsets
account = databricksAccountHandler(account_id = account_id, username = username, password = password)
start_month = datetime.now() - pd.offsets.DateOffset(months=2)
end_month = datetime.now() - pd.offsets.DateOffset(months=1)

# COMMAND ----------

df = account.fetch_usage(start_month=start_month, end_month=end_month)

# COMMAND ----------

df.display()
