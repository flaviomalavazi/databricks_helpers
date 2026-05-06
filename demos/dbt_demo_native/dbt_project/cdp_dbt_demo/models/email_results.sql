{{ config(materialized='table') }}

select
    ad_id
    ,investment_interval_hourly
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,emails_cost
    ,emails_sent
    ,emails_delivered
    ,emails_sent/nullif(emails_delivered,0)           as email_delivery_rate
    ,emails_bounced
    ,emails_bounced/nullif(emails_sent,0)             as email_bounce_rate
    ,emails_opened
    ,emails_opened/nullif(emails_delivered,0)         as email_open_rate
    ,emails_clicked
    ,emails_clicked/nullif(emails_opened,0)           as email_click_trough_rate
    ,emails_unsubscribed
    ,emails_unsubscribed/nullif(emails_delivered,0)   as email_unsubscribe_rate
from
    {{ ref('vw_mailchimp') }}
