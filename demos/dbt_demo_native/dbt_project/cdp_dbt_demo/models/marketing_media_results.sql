select
    cm.ad_id
    ,cm.investment_interval_hourly
    ,cm.utm_source                              as utm_source
    ,cm.utm_campaign                            as utm_campaign
    ,cm.utm_content                             as utm_content
    ,sum(cm.spend)                              as ad_cost_per_hour
    ,sum(cm.impressions)                        as impressions_per_period
    ,sum(cm.clicks)                             as clicks_per_period
    ,count(ma.media_payment_event_id)           as total_payments_found
    ,sum(ma.transaction_bill_value)             as total_payments_value
from 
    {{ ref('consolidated_media') }} as cm
    left join {{ ref('marketing_attribution') }} as ma on ma.media_attributed_first_ad = cm.ad_id and cm.investment_interval_hourly = ma.web_analytics_event_hour
group by
    all
