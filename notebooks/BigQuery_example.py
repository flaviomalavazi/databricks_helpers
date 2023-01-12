# Databricks notebook source
# MAGIC %md
# MAGIC # Configuração do cluster que vai acessar o BQ:
# MAGIC
# MAGIC Assumindo que a configuração da Service Account que fará a leitura dos dados do BQ já está pronta, e seus
# MAGIC acessos configurados dentro do warehouse, podemos passar diretamente para o passo 2
# MAGIC [desta documentação](https://docs.databricks.com/external-data/bigquery.html#step-2-set-up-databricks), e
# MAGIC fazer a configuração das chaves de acesso do BigQuery em um Cluster Databricks que fará a leitura dos dados.
# MAGIC
# MAGIC Um exemplo prático dos parâmetros e valores que devem ser inseridos na aba `spark` da configuração do cluster
# MAGIC Databricks, com valores fictícios para as credenciais, pode ser encontrado abaixo:
# MAGIC
# MAGIC ```
# MAGIC credentials string_em_base64
# MAGIC spark.hadoop.fs.gs.auth.service.account.email sua_service_account@seu_projeto.iam.gserviceaccount.com
# MAGIC spark.hadoop.fs.gs.project.id seu_projeto
# MAGIC spark.hadoop.google.cloud.auth.service.account.enable true
# MAGIC spark.hadoop.fs.gs.auth.service.account.private.key -----BEGIN PRIVATE KEY-----\nSTRING_BEM_COMPRIDA\n-----END PRIVATE KEY-----\n
# MAGIC spark.hadoop.fs.gs.auth.service.account.private.key.id STRING_ALFANUMERICA_COM_40_CARACTERES
# MAGIC
# MAGIC ```
# MAGIC
# MAGIC
# MAGIC Para auxiliar na geração do base64 do json de credenciais do BQ, pode ser utilizado o código abaixo:

# COMMAND ----------
# MAGIC %md
# MAGIC ## Definição de funções para tratar o json de credenciais do BigQuery
# MAGIC ## para base64 e criar nosso objeto para interagir com o Warehouse
# MAGIC Na célula abaixo, definimos uma função para gerar uma string em base64
# MAGIC a partir do Json de credenciais do BQ e também definimos a classe
# MAGIC BigQuery, que será responsável por validar se o cluster Databricks está
# MAGIC corretamente configurado para conectar com o BQ e interagir com ele
# MAGIC executando leituras de tabelas inteiras ou queries específicas.

# COMMAND ----------
%run ../library/data_warehouses/big_query


# COMMAND ----------

big_query_credentials = {
    "type": "service_account",
    "project_id": "seu_projeto",
    "private_key_id": "string_alfanumerica_com_40_caracteres",
    "private_key": "-----BEGIN PRIVATE KEY-----\nstring_alfanumerica_com_caracteres_especiais\n-----END PRIVATE KEY-----\n",
    "client_email": "sua_service_account@seu_projeto.iam.gserviceaccount.com",
    "client_id": "numero_do_client_id_google",
    "auth_uri": "https://accounts.google.com/o/oauth2/auth",
    "token_uri": "https://oauth2.googleapis.com/token",
    "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
    "client_x509_cert_url": "https://www.googleapis.com/robot/v1/metadata/x509/sua_service_account%40seu_projeto.iam.gserviceaccount.com",
}

credentials_generator(big_query_credentials)

# COMMAND ----------

# MAGIC %md
# MAGIC # Configuração dos parâmetros do cluster usando secrets do Databricks para evitar exposição de credenciais
# MAGIC Como as variáveis do spark para acesso ao BQ contém as credenciais da conta de serviço, não é interessante
# MAGIC expormos isso em texto simples, para evitar fazer isso, podemos empregar os Databricks secrets, para fazer
# MAGIC isso, vamos criar um secret scope com o [databricks-cli](https://docs.databricks.com/dev-tools/cli/index.html)
# MAGIC
# MAGIC O comando para criar o secret scope é:
# MAGIC
# MAGIC ```Databricks secrets create-scope --scope <scope-name>```
# MAGIC
# MAGIC Para seguirmos com os exemplos passo a passo, vamos usar o nome `bq_demo` como nosso nome de escopo, desta
# MAGIC forma, a criação do escopo fica:
# MAGIC
# MAGIC ```Databricks secrets create-scope --scope bq_demo```
# MAGIC
# MAGIC
# MAGIC Uma vez criado nosso escopo, podemos colocar as chaves que precisaremos para acessar o BigQuery, a sintaxe
# MAGIC para acrescentar chaves é:
# MAGIC
# MAGIC ```Databricks secrets put --scope <scope-name> --key <key-name>```
# MAGIC
# MAGIC Então, vamos começar colocando a chave de credenciais (que vamos chamar de `base64_credentials`):
# MAGIC
# MAGIC ```Databricks secrets put --scope bq_demo --key base64_credentials``` -- Colocando o json em base64
# MAGIC (obtido com o código acima)
# MAGIC
# MAGIC
# MAGIC E acrescentaremos as demais chaves necessárias depois:
# MAGIC <br>
# MAGIC ```Databricks secrets put --scope bq_demo --key private_key``` -- Colocando a private_key (chave no json: `private_key`)
# MAGIC <br>
# MAGIC ```Databricks secrets put --scope bq_demo --key private_key_id``` -- Colocando a private_key_id do json de configurações (chave no json: `private_key_id`)
# MAGIC <br>
# MAGIC ```Databricks secrets put --scope bq_demo --key project_id``` -- Colocando o project_id da GCP (chave no json: `project_id`)
# MAGIC <br>
# MAGIC ```Databricks secrets put --scope bq_demo --key service_account_email``` -- Colocando o email da service account que vai fazer os acessos (chave no json: `client_email`)

# COMMAND ----------

# DBTITLE 1,Testando se nossas chaves foram criadas no escopo de segredos
dbutils.secrets.list("bq_demo")  # noqa: F821

# COMMAND ----------

# MAGIC %md
# MAGIC # Utilizando escopos de segredos nas configurações do cluster
# MAGIC
# MAGIC Agora que configuramos nosso escopo de segredos, podemos usar a seguinte estrutura para configurar nosso
# MAGIC cluster (na aba de configurações avançadas->spark):
# MAGIC ```
# MAGIC credentials {{secrets/bq_demo/base64_credentials}}
# MAGIC spark.hadoop.fs.gs.auth.service.account.email {{secrets/bq_demo/service_account_email}}
# MAGIC spark.hadoop.fs.gs.project.id {{secrets/bq_demo/project_id}}
# MAGIC spark.hadoop.google.cloud.auth.service.account.enable true
# MAGIC spark.hadoop.fs.gs.auth.service.account.private.key {{secrets/bq_demo/private_key}}
# MAGIC spark.hadoop.fs.gs.auth.service.account.private.key.id {{secrets/bq_demo/private_key_id}}
# MAGIC
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC # Utilizando parametros de leitura do big query apenas em um notebook específico
# MAGIC Há vezes em que não queremos que o big query fique disponível para leitura por todos os usuários de um
# MAGIC determinado cluster, para evitar isso, podemos configurar as os parâmetros necessários para acessar o
# MAGIC warehouse diretamente no notebook, com a sintaxe abaixo:
# MAGIC ```
# MAGIC spark.conf.set("credentials", dbutils.secrets.get("bq_demo", "base64_credentials"))
# MAGIC spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
# MAGIC spark.conf.set("spark.hadoop.fs.gs.auth.service.account.email", dbutils.secrets.get("bq_demo", "service_account_email"))
# MAGIC spark.conf.set("spark.hadoop.fs.gs.project.id", dbutils.secrets.get("bq_demo", "project_id"))
# MAGIC spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key", dbutils.secrets.get("bq_demo", "private_key"))
# MAGIC spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key.id", dbutils.secrets.get("bq_demo", "private_key_id"))
# MAGIC ```
# MAGIC Configurando desta forma, apenas a sessão do notebook que fez a configuração terá acesso ao BQ, e caso o
# MAGIC cluster seja reiniciado, os parâmetros serão perdidos e deverão ser definidos novamente.

# COMMAND ----------

# Versão em código para configurar o cluster a nível de sessão
# spark.conf.set("credentials", dbutils.secrets.get("bq_demo", "base64_credentials"))
# spark.conf.set("spark.hadoop.google.cloud.auth.service.account.enable", "true")
# spark.conf.set("spark.hadoop.fs.gs.auth.service.account.email", dbutils.secrets.get("bq_demo", "service_account_email"))
# spark.conf.set("spark.hadoop.fs.gs.project.id", dbutils.secrets.get("bq_demo", "project_id"))
# spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key", dbutils.secrets.get("bq_demo", "private_key"))
# spark.conf.set("spark.hadoop.fs.gs.auth.service.account.private.key.id", dbutils.secrets.get("bq_demo", "private_key_id"))

# COMMAND ----------

# MAGIC %md
# MAGIC # Validando a configuração no cluster Databricks que fará a leitura do BigQuery
# MAGIC O código abaixo valida se os parâmetros de acesso do big query foram configurados corretamente no cluster que
# MAGIC está sendo utilizado para ler do big query, caso seja levantada uma exceção no código abaixo, muito
# MAGIC provavelmente a leitura falhará por erros na configuração do cluster.
# MAGIC
# MAGIC Em caso de passagem sem erros pelo código abaixo, ainda podemos ter problemas de permissionamento para o
# MAGIC usuário de serviço no BigQuery, que devem ser ajustados no projeto de BQ na conta da GCP.

# COMMAND ----------

# MAGIC %md
# MAGIC # Lendo dados do BigQuery
# MAGIC
# MAGIC Para ler os dados de uma tabela inteira do big query, podemos utilizar a estrutura abaixo, que foi tirada
# MAGIC [da documentação do Databricks](https://docs.databricks.com/external-data/bigquery.html#google-bigquery)


# COMMAND ----------

# DBTITLE 1,Lendo nosso nome de projeto do BQ para uma variável
parent_project = dbutils.secrets.get("bq_demo", "project_id")
big_query_project = parent_project  # Para este exemplo, estamos trabalhando em um big query com o mesmo nome de seu parent project

project_options = {
    "project": big_query_project,  # Projeto do BQ
    "parentProject": parent_project,  # Projeto pai do BQ (neste exemplo, é o mesmo do BQ)
}

bigQuery = BigQuery(project_options=project_options)

# COMMAND ----------

dataset = "demo_dataset"
table_name = "demo_table"
table_id = f"{big_query_project}.{dataset}.{table_name}"
query = f"""select
            *
        from
            {big_query_project}.demo_dataset.demo_table
        where
            condition_column is true"""
temp_schema = "bq_demo_temp_dataset"

# COMMAND ----------

# DBTITLE 1,Lendo uma tabela completa do BQ para o spark
df = bigQuery.read(table_id=table_id)
df.limit(20).display()

# COMMAND ----------

# MAGIC %md
# MAGIC # Lendo dados no BQ utilizando queries
# MAGIC
# MAGIC Para ler dados no BQ a partir de uma query, precisamos de um dataset temporário que será usado
# MAGIC para materializar os resultados da query antes deles serem carregados para o spark.
# MAGIC
# MAGIC Este e mais exemplos de leitura também podem ser encontrados no [link de documentação da Databricks](https://docs.databricks.com/external-data/bigquery.html#google-bigquery-python-sample-notebook)

# COMMAND ----------

# DBTITLE 1,Lendo os dados no BQ utilizando uma query
df = bigQuery.read(query=query, materialization_dataset=temp_schema)
df.limit(20).display()
