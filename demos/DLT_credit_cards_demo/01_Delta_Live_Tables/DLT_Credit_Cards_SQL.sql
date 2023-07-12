-- Databricks notebook source
-- MAGIC %md
-- MAGIC # Criando um pipeline de Delta Live Tables com expectativas de qualidade de dados em SQL
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC Neste notebook, fazemos a ingestão de dados JSON que estão sendo gerados pelo notebook de exemplo (`Credit_Card_Transaction_Data_Generator`) para uma tabela de landing utilizando o Auto-loader, e depois fazemos o tratamento dos dados para garantir que não estamos propagando registros que não atendam nossas regras de negócios.

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Definindo os widgets do nosso Delta Live Tables Pipeline
-- MAGIC Definimos parâmetros que serão utilizados em nosso pipeline de dados para buscarmos os dados na landing zone e definirmos:
-- MAGIC - O caminho dos arquivos fonte
-- MAGIC - O formato dos arquivos fonte
-- MAGIC - O nome da tabela bronze
-- MAGIC - A opção de inferir os tipos das colunas de nossos arquivos fonte
-- MAGIC - O nome da tabela prata

-- COMMAND ----------

-- CREATE WIDGET TEXT landing_path DEFAULT 'dbfs:/Users/<SEU_EMAIL>/demos/dlt_credit_cards/';
-- CREATE WIDGET TEXT bronze_data_format DEFAULT 'json';
-- CREATE WIDGET TEXT bronze_table_name DEFAULT 'landing_credit_card_transaction';
-- CREATE WIDGET TEXT bronze_infer_column_types DEFAULT 'true';
-- CREATE WIDGET TEXT clean_silver_table DEFAULT 'merchant_credit_card_transactions';
-- CREATE WIDGET TEXT aggregated_gold_table DEFAULT 'gold_merchant_credit_card_transactions_daily_agg';

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###2. Ingerindo os dados em JSON para a nossa tabela bronze incremental (Delta)
-- MAGIC Declaramos nossa tabela incremental com a sintaxe de `CREATE STREAMING LIVE TABLE` e passando os parâmetros de caminho e formato dos dados de origem.

-- COMMAND ----------

CREATE STREAMING LIVE TABLE ${bronze_table_name}
AS
SELECT * FROM cloud_files("${landing_path}", "${bronze_data_format}")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###3. Criando nossa tabela prata, incremental e com expectativas de qualidade
-- MAGIC Aqui, vamos declarar a nossa tabela prata limpa, que aplica regras de negócio para garantir que nossos processos que dependem dos dados que estamos ingerindo terão informações íntegras para trabalhar.
-- MAGIC Também aplicamos transformações dos tipos dos dados para que os processos dependentes não precisem se preocupar com gestão de formatos de dados.

-- COMMAND ----------

CREATE STREAMING LIVE TABLE ${clean_silver_table} (
  -- Nome do estabelecimento não pode ser vazio
  CONSTRAINT merchant_name_not_null EXPECT (merchant_name IS NOT NULL) ON VIOLATION DROP ROW
  -- A bandeira do cartão precisa estar na lista de cartões pré aprovados
  ,CONSTRAINT card_network_in_approved_list EXPECT (card_network in ('Mastercard', 'Visa', 'Elo', 'Amex')) ON VIOLATION DROP ROW
  -- Número de parcelas tem que ser maior do que zero
  ,CONSTRAINT installments_greater_than_zero EXPECT (installments > 0) ON VIOLATION DROP ROW
  -- Validando o BIN (primeiros dígitos) dos cartões de crédito com as bandeiras de cada um deles 
  ,CONSTRAINT card_bin_in_card_network EXPECT ((card_network = 'Mastercard' and card_bin in (51, 52, 53, 54)) or (card_network = 'Visa' and card_bin in (4)) or (card_network = 'Amex' and card_bin in (34, 37)) or (card_network = 'Elo' and card_bin in (636368, 636369, 438935, 504175, 451416, 636297, 5067, 4576, 4011, 506699))) ON VIOLATION DROP ROW
  -- Garantindo que o nome da pessoa dona do cartão está preenchido corretamente
  ,CONSTRAINT card_holder_not_null EXPECT (card_holder is not NULL) ON VIOLATION DROP ROW
  -- O valor da conta tem que ser maior que zero se for um pagamento, ou menor que zero se for um chargeback (devolução)
  ,CONSTRAINT bill_value_valid EXPECT (((bill_value > 0) and (transaction_type = 'expense')) or ((bill_value < 0) and (transaction_type = 'chargeback'))) ON VIOLATION DROP ROW
  -- A data de validade do cartão tem que ser posterior à data da transação
  ,CONSTRAINT card_expiration_date_valid EXPECT (card_expiration_date > `timestamp`) ON VIOLATION DROP ROW
)
AS
SELECT 
  transaction_id
  ,type AS transaction_type
  ,to_timestamp(`timestamp`) as `timestamp`
  ,merchant_type
  ,merchant_name
  ,card_holder
  ,currency
  ,card_network
  ,CAST(card_bin as LONG) AS card_bin
  ,CAST(bill_value as DOUBLE) AS bill_value
  ,CAST(installments AS INT) as installments
  ,last_day(to_date(card_expiration_date,"MM/yy")) AS card_expiration_date
FROM 
  stream(live.${bronze_table_name})

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ###4. Agregando nossos dados diariamente para melhorar a performance (e reduzir custos) para o time de negócios que fará as análises de vendas

-- COMMAND ----------

CREATE LIVE TABLE ${aggregated_gold_table} (
  merchant_type STRING COMMENT "Merchant type that was involved in the transactions",
  card_network STRING COMMENT "Card network that issued the credit cards that made the transactions",
  timestamp_day DATE COMMENT "Date in which the transactions happened",
  sum_bill_value DOUBLE COMMENT "Sum of the amount that was transactioned in that date for that card network and merchant"
)

  COMMENT "Daily aggregated gold table for all transactional data grouped by card network and merchant type"

AS

SELECT
  merchant_type
  ,card_network
  ,CAST(date_trunc('DAY', `timestamp`) AS DATE)   as timestamp_day
  ,SUM(bill_value)                                as sum_bill_value
FROM
  LIVE.${clean_silver_table}
GROUP BY
  ALL