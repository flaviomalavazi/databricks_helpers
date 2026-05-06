{{ config(materialized='table') }}

with tab_email_normalized AS (
select
    ad_id
    ,investment_interval_hourly
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,emails_cost                    as spend
    ,emails_opened                  as impressions
    ,(emails_opened/1000.0)/spend   as cpm
    ,emails_clicked                 as clicks
    ,(clicks)/spend                 as cpc
    ,(clicks*1.0)/impressions       as ctr
from
    {{ ref('vw_mailchimp') }}
)

select * from {{ ref('vw_bing_ads') }}
union all
select * from {{ ref('vw_facebook_ads') }}
union all
select * from {{ ref('vw_google_ads') }}
union all
select * from tab_email_normalized


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
