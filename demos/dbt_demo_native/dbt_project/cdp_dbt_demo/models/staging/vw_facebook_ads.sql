select 
    ad_id
    ,date_trunc("hour", investment_interval) as investment_interval_hourly
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,spend
    ,impressions
    ,(impressions/1000.0)/nullif(spend,0) as cpm
    ,clicks
    ,(clicks)/nullif(spend,0)             as cpc
    ,(clicks*1.0)/nullif(impressions,0)   as ctr
from
    {{ source('raw', 'tab_facebook_investment') }}
where
    last_update_at = (select max(last_update_at) from {{ source('raw', 'tab_facebook_investment') }})