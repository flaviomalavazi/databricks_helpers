select
    ad_id
    ,date_trunc("hour", investment_interval) as investment_interval_hourly
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,emails_cost
    ,emails_sent
    ,emails_delivered
    ,emails_bounced
    ,emails_opened
    ,emails_clicked
    ,emails_unsubscribed
from
    {{ source('raw', 'tab_mailchimp_emails') }}
where
    last_update_at = (select max(last_update_at) from {{ source('raw', 'tab_mailchimp_emails') }})
