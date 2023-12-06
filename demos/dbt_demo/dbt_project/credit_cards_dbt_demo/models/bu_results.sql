{{ config(materialized='table') }}

select
    transaction_merchant_type                                                                   as transaction_merchant_type,
    date_trunc("day", transaction_timestamp)                                                    as transaction_date,
    transaction_card_network                                                                    as transaction_card_network,
    count(distinct transaction_id)                                                              as bu_distinct_transactions,
    count(distinct customer_id)                                                                 as bu_distinct_customers,
    count(distinct case when transaction_type = "expense" then transaction_id else null end)    as bu_distinct_expense_transactions,
    count(distinct case when transaction_type = "chargeback" then transaction_id else null end) as bu_distinct_chargeback_transactions,
    sum(transaction_bill_value)                                                                 as bu_total_bill_value,
    avg(transaction_bill_value)                                                                 as bu_average_bill_value,
    min(transaction_timestamp)                                                                  as bu_first_transaction_at,
    max(transaction_timestamp)                                                                  as bu_latest_transaction_at
from
    {{ ref('credit_card_transactions') }}
group by
    transaction_merchant_type,
    transaction_date,
    transaction_card_network