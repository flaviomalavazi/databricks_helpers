SELECT
    session_id
    ,ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) AS session_event_order
    ,event_timestamp
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,utm_medium
    ,customer_id
    ,ad_id
    ,event_id
    ,browser_name
    ,browser_user_agent
    ,click_id
    ,device_is_mobile
    ,device_type
    ,event_type
    ,geo_country
    ,os_name
    ,page_url
    ,page_url_path
    ,referer_medium
    ,referer_url
    ,user_domain_id
from
    {{ ref('vw_web_sessions') }}