SELECT
    session_id
    ,ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY event_timestamp) AS session_event_order
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,customer_id
    ,ad_id
    ,event_id
    ,event_timestamp
    ,page_url
    ,page_url_path
    ,page_url_domain
    ,browser  
from
    {{ ref('vw_web_sessions') }}