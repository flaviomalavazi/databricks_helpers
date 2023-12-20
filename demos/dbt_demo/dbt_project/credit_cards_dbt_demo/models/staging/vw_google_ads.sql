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
    lakehouse_federation_bigquery.flavio_malavazi.tab_google_ads
where
    last_update_at = (select max(last_update_at) from lakehouse_federation_bigquery.flavio_malavazi.tab_google_ads)