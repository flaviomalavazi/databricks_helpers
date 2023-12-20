select
  date_trunc('day', investment_interval_hourly) as investment_day,
  sum(costs) as total_cost,
  sum(conversions) as number_of_conversions,
  sum(revenue_on_investment) as total_revenue_on_investment,
  100*(total_revenue_on_investment/nullif(total_cost,0)) as marketing_roi
from
  flavio_malavazi.dbt_credit_cards_demo.marketing_results
group by
    investment_day