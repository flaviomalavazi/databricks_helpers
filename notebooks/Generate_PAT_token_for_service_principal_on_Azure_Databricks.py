# Databricks notebook source
# MAGIC %md
# MAGIC # Criando um PAT token para ser utilizado por um service principal na Azure

# COMMAND ----------

dbutils.widgets.text("lifetime_seconds", "7200", "Duração desejada do Token Databricks")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Importando Bibliotecas

# COMMAND ----------

import requests
import json

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Gerando um token de OAuth do Azure AAD para autenticar a chamada para a API do Databricks

# COMMAND ----------

url = f"https://login.microsoftonline.com/{dbutils.secrets.get('service_principal', 'tenant_id')}/oauth2/v2.0/token" # Tenant ID da XP
headers = {'Content-Type': 'application/x-www-form-urlencoded'}

data = {
    'client_id': dbutils.secrets.get("service_principal", "client_id"), # Do Service Principal que será utilizado
    'grant_type': "client_credentials",
    'scope': '2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default', # Padrão da Azure
    'client_secret': dbutils.secrets.get("service_principal", "client_secret")
}

response = requests.post(url, headers=headers, data=data)
token_aad = response.json()["access_token"]

# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Gerando um PAT token do Databricks com uma duração determinada (diferente da 1h padrão do AAD)

# COMMAND ----------

workspace_url = f"https://{dbutils.notebook.entry_point.getDbutils().notebook().getContext().browserHostName().getOrElse(None)}" # Workspace que emitirá o token para o Service Principal

lifetime_seconds = int(dbutils.widgets.get("lifetime_seconds")) if dbutils.widgets.get("lifetime_seconds") != "" else 7200 # Default = 2h, mas pode ser alterado no widget acima ou diretamente na variável

header = {'Authorization': 'Bearer {}'.format(token_aad)}
endpoint = '/api/2.0/token/create'
payload = json.dumps({'lifetime_seconds': lifetime_seconds,'comment': 'string'})
 
resp = requests.post(
  workspace_url + endpoint,
  data=payload,
  headers=header
)

# COMMAND ----------

resp.text
