
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}

with customer_records as (

select 
    sha2(email, 256)    as customer_id,
    sha2(name, 256)     as customer_name,
    sha2(address, 256)  as customer_address,
    city                as customer_city,
    state               as customer_state,
    lucky_number        as customer_lucky_number,
    record_created_at   as customer_record_created_at,
    last_update_at      as customer_record_last_update_at
from
    flavio_malavazi.dbt_credit_cards_demo_raw.tab_customer_records

), customer_transactions as (

select
    customer_id                         as customer_id,
    sum(transaction_bill_value)         as customer_total_value_transactioned,
    avg(transaction_bill_value)         as customer_average_value_transactioned,
    min(transaction_timestamp)          as customer_first_transaction_at,
    max(transaction_timestamp)          as customer_latest_transaction_at,
    count(distinct transaction_id)      as customer_transactions
from
    {{ ref('credit_card_transactions') }}
group by
    customer_id

), customer_web_events as (

select
    sha2(user_custom_id, 256) as customer_id,
    min(event_timestamp) as customer_first_web_interaction,
    max(event_timestamp) as customer_latest_web_interaction,
    count(distinct event_id) as customer_distinct_interactions
from
    lakehouse_federation_bigquery.flavio_malavazi.tab_web_events
group by
    customer_id
)


select 
    cr.customer_id,
    cr.customer_name,
    cr.customer_address,
    cr.customer_city,
    cr.customer_state,
    cr.customer_lucky_number,
    cr.customer_record_created_at,
    cr.customer_record_last_update_at,
    cwe.customer_first_web_interaction,
    cwe.customer_latest_web_interaction,
    cwe.customer_distinct_interactions,
    ct.customer_total_value_transactioned,
    ct.customer_average_value_transactioned,
    ct.customer_first_transaction_at,
    ct.customer_latest_transaction_at,
    ct.customer_transactions,
    now() as last_update_at
from
    customer_records as cr
    left join customer_web_events as cwe on cwe.customer_id = cr.customer_id
    left join customer_transactions as ct on ct.customer_id = cr.customer_id

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
