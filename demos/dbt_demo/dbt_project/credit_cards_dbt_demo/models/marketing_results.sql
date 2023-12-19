select
    cm.ad_id
    ,date_trunc("hour", cm.investment_interval) as investment_interval
    ,cm.utm_source                              as utm_source
    ,cm.utm_campaign                            as utm_campaign
    ,cm.utm_content                             as utm_content
    ,sum(cm.spend)                              as ad_cost_per_hour
    ,sum(cm.impressions)                        as impressions_per_period
    ,sum(cm.clicks)                             as clicks_per_period
    ,count(ma.media_payment_event_id)           as total_payments_found
    ,sum(ma.transaction_bill_value)             as total_payments_value
from 
    flavio_malavazi.dbt_credit_cards_demo.consolidated_media as cm
    left join flavio_malavazi.dbt_credit_cards_demo.marketing_attribution as ma on ma.media_attributed_first_ad = cm.ad_id and cm.investment_interval = date_trunc("hour", ma.transaction_timestamp)
group by
    all
