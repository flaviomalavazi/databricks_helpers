select
    ad_id
    ,investment_interval
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