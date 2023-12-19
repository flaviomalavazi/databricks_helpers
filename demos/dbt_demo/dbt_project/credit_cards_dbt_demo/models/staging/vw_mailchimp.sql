select
    ad_id
    ,investment_interval
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
    flavio_malavazi.dbt_credit_cards_demo_raw.tab_mailchimp
where
    last_update_at = (select max(last_update_at) from flavio_malavazi.dbt_credit_cards_demo_raw.tab_mailchimp)