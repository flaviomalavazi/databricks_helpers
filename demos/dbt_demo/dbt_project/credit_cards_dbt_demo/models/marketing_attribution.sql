with payment_events as (

select
    ws.session_id
    ,ws.customer_id
    ,first_value(ad_id) OVER (PARTITION BY ws.session_id ORDER BY ws.event_timestamp)                      as first_ad_id
    ,first_value(utm_source) OVER (PARTITION BY ws.session_id ORDER BY ws.event_timestamp)                 as first_utm_source
    ,first_value(utm_campaign) OVER (PARTITION BY ws.session_id ORDER BY ws.event_timestamp)               as first_utm_campaign
    ,first_value(utm_content) OVER (PARTITION BY ws.session_id ORDER BY ws.event_timestamp)                as first_utm_content
    ,first_value(ws.event_timestamp) OVER (PARTITION BY ws.session_id ORDER BY ws.event_timestamp DESC)    as payment_event_at_hour
    ,first_value(ws.event_id) OVER (PARTITION BY ws.session_id ORDER BY ws.event_timestamp DESC)           as web_analitics_payment_event_id
from
    {{ ref('web_sessions') }} ws
    inner join (
        select
            distinct session_id
        from
            {{ ref('web_sessions') }}
        where
            page_url_path =  'confirmation/'
        ) as payment_sessions on payment_sessions.session_id = ws.session_id

), payments as (

select
    transaction_card_network
    ,customer_id
    ,transaction_merchant_type
    ,transaction_bill_value
    ,transaction_id as payment_event_id
    ,transaction_timestamp
from
    {{ ref('credit_card_transactions') }}

)

select
    pe.session_id                                       as media_session_id_with_payment
    ,pe.first_ad_id                                     as media_attributed_first_ad
    ,pe.first_utm_source                                as media_attributed_first_utm_source
    ,pe.first_utm_campaign                              as media_attributed_first_utm_campaign
    ,pe.first_utm_content                               as media_attributed_first_utm_content
    ,p.payment_event_id                                 as media_payment_event_id
    ,COALESCE(p.customer_id, pe.customer_id)            as customer_id
    ,p.transaction_card_network                         as transaction_card_network
    ,p.transaction_merchant_type                        as transaction_merchant_type
    ,p.transaction_bill_value                           as transaction_bill_value
    ,p.transaction_timestamp                            as transaction_timestamp
    ,date_trunc("hour", pe.payment_event_at_hour)       as web_analytics_event_hour
from
    payments p
    left join payment_events pe on p.payment_event_id = pe.web_analitics_payment_event_id
group by
    all
