{{ config(materialized='table') }}

select
    ad_id
    ,investment_interval
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,emails_cost
    ,emails_sent
    ,emails_delivered
    ,emails_sent/emails_delivered           as email_delivery_rate
    ,emails_bounced
    ,emails_bounced/emails_sent             as email_bounce_rate
    ,emails_opened
    ,emails_opened/emails_delivered         as email_open_rate
    ,emails_clicked
    ,emails_clicked/emails_opened           as email_click_trough_rate
    ,emails_unsubscribed
    ,emails_unsubscribed/emails_delivered   as email_unsubscribe_rate
from
    {{ ref('vw_mailchimp') }}
