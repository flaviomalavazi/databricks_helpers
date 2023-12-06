{{ config(materialized='table') }}

select 
  transaction_id,
  sha2(customer_id, 256)                           as customer_id,
  type                                             as transaction_type,
  to_timestamp(`timestamp`)                        as transaction_timestamp,
  merchant_type                                    as transaction_merchant_type,
  merchant_name                                    as transaction_merchant_name,
  sha2(card_holder, 256)                           as transaction_card_holder,
  currency                                         as transaction_currency,
  card_network                                     as transaction_card_network,
  cast(card_bin as LONG)                           as transaction_card_bin,
  cast(bill_value as DOUBLE)                       as transaction_bill_value,
  cast(installments as INT)                        as transaction_installments,
  last_day(to_date(card_expiration_date,"MM/yy"))  as transaction_card_expiration_date,
  now()                                            as last_update_at
from
  flavio_malavazi.dbt_credit_cards_demo_raw.tab_sale_transactions
