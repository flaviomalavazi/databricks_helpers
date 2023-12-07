{{ config(materialized='table') }}

select
    ad_id
    ,investment_interval
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
    flavio_malavazi.dbt_credit_cards_demo_raw.tab_mailchimp
where
    last_update_at = (select max(last_update_at) from flavio_malavazi.dbt_credit_cards_demo_raw.tab_mailchimp)
