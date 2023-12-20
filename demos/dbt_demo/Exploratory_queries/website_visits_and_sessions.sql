select
  date_trunc('day', event_timestamp) as visit_day,
  utm_source,
  case when utm_source = 'organic' then 'organic' else utm_campaign end as utm_campaign,
  case when utm_source = 'organic' then 'organic' else utm_content end as utm_content,
  count(distinct customer_id) as visitors,
  count(distinct session_id) as sessions
from
  flavio_malavazi.dbt_credit_cards_demo.web_sessions
group by
    all
