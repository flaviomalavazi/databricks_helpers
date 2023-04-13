# Databricks notebook source
# MAGIC %md
# MAGIC # Getting PowerBI's IP Ranges Programatically
# MAGIC Applications:
# MAGIC - AWS Databricks Workspaces that have access controlled by IP Allow Lists and want to allow Azure PowerBI to connect to their DBSQL Warehouses without a Microsoft on-premises data gateway.

# COMMAND ----------

# DBTITLE 1,Installing BeautifulSoup
# MAGIC %sh
# MAGIC pip install beautifulsoup4 --quiet --disable-pip-version-check

# COMMAND ----------

# DBTITLE 1,Function definition
from bs4 import BeautifulSoup
import re
import requests

def get_latest_list_of_ips_pbi():
    ip_download_page = "https://www.microsoft.com/en-us/download/confirmation.aspx?id=56519"
    response = requests.get(ip_download_page, stream=True)

    # Decode the page content from bytes to string
    html = response.content.decode("utf-8")
    soup = BeautifulSoup(html, 'html.parser')

    # Finding a pattern(certain text)
    pattern = 'click here to download manually'

    # Anchor tag
    ip_download_link = soup.find_all('a', text = pattern, href=True)[0]["href"]

    response = requests.get(ip_download_link)
    if response.status_code == 200:
        ip_dict = json.loads(response.content.decode("utf-8"))
    else:
        raise Exception("Error downloading the list")

    pbi_object = [service for service in ip_dict["values"] if service["name"] == "PowerBI"][0]
    pbi_ip_range = pbi_object["properties"]["addressPrefixes"]

    return pbi_ip_range

# COMMAND ----------

# DBTITLE 1,Getting the list of IPs for PowerBI
list_of_ips = get_latest_list_of_ips_pbi()

# COMMAND ----------

list_of_ips
