with marketing as (select
    utm_source,
    investment_interval_hourly,
    utm_campaign,
    utm_content,
    sum(ad_cost_per_hour) as costs,
    sum(total_payments_found) as conversions,
    sum(total_payments_value) as revenue_on_investment
from
  {{ ref('marketing_media_results') }}
group by
    utm_source,
    investment_interval_hourly,
    utm_campaign,
    utm_content
), organic as (
    select
        'organic'                                   as utm_source,
        web_analytics_event_hour                    as investment_interval_hourly,
        'organic'                                   as utm_campaign,
        'organic'                                   as utm_content,
        sum(0)                                      as costs,
        count(distinct media_payment_event_id)      as conversions,
        sum(transaction_bill_value)                 as revenue_on_investment
    from
        {{ ref('marketing_attribution') }}
    where
        media_attributed_first_utm_source = 'organic'
    group by
        all
)

select *, revenue_on_investment/nullif(costs,0) as roi from marketing
union all
select *, revenue_on_investment/nullif(costs,0) as roi from organic
