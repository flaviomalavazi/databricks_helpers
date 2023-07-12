SELECT
  timestamp_day as time_period,
  merchant_type,
  card_network,
  sum_bill_value
FROM
  YOUR_CATALOG.YOUR_SCHEMA.gold_merchant_credit_card_transactions_daily_agg