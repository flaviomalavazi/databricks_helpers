with payment_sessions as (
select
    session_id                          
    ,first(ad_id)                                                                       as first_ad_id
    ,case when last(page_url_path) = '/confirmation' then last(event_id) else null end  as payment_event_id
    ,first(customer_id) as customer_id
from
    {{ ref('web_sessions') }}
group by session_id
), payments as (
    select
        transaction_card_network
        ,transaction_merchant_type
        ,transaction_bill_value
        ,transaction_id as payment_event_id
        ,transaction_timestamp
    from
        {{ ref('credit_card_transactions') }}
)

select
    ps.session_id                                                                               as media_session_id_with_payment
    ,ps.first_ad_id                                                                             as media_attributed_first_ad
    ,COALESCE(ps.first_ad_id, specific_events.ad_id)                                            as media_raw_event_ad_id
    ,CASE WHEN media_attributed_first_ad != media_raw_event_ad_id THEN TRUE ELSE FALSE END      as media_attribution_flag
    ,ps.payment_event_id                                                                        as media_payment_event_id
    ,COALESCE(ps.customer_id, specific_events.customer_id)                                      as customer_id
    ,COALESCE(p.transaction_card_network, specific_payments.transaction_card_network)           as transaction_card_network
    ,COALESCE(p.transaction_merchant_type, specific_payments.transaction_merchant_type)         as transaction_merchant_type
    ,COALESCE(p.transaction_bill_value, specific_payments.transaction_bill_value)               as transaction_bill_value
    ,COALESCE(p.transaction_timestamp, specific_payments.transaction_timestamp)                 as transaction_timestamp
from
    payment_sessions ps
    left join payments p on p.payment_event_id = ps.payment_event_id
    left join {{ ref('web_sessions') }} specific_events on specific_events.event_id = ps.payment_event_id
    left join payments specific_payments on specific_payments.payment_event_id = specific_events.event_id
