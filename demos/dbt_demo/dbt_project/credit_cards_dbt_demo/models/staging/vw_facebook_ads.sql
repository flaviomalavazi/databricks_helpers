select 
    ad_id
    ,date_trunc("hour", investment_interval) as investment_interval_hourly
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,spend
    ,impressions
    ,(impressions/1000.0)/spend as cpm
    ,clicks
    ,(clicks)/spend             as cpc
    ,(clicks*1.0)/impressions   as ctr
from
    flavio_malavazi.dbt_credit_cards_demo_raw.tab_facebook_ads
where
    last_update_at = (select max(last_update_at) from flavio_malavazi.dbt_credit_cards_demo_raw.tab_facebook_ads)