SELECT
    date_trunc('hour', event_timestamp) || sha2(user_custom_id,256) as session_id
    ,utm_source
    ,CASE
        WHEN page_url_path = '/home' AND utm_source != 'organic' THEN sha2(utm_source || "-" || utm_campaign || "-" || utm_content, 256)
        ELSE NULL
        END                                                                                         AS ad_id
    ,case when (utm_source = 'organic' OR page_url_path != '/home') THEN NULL ELSE utm_campaign END AS utm_campaign
    ,case when (utm_source = 'organic' OR page_url_path != '/home') THEN NULL ELSE utm_medium END   AS utm_medium
    ,case when (utm_source = 'organic' OR page_url_path != '/home') THEN NULL ELSE utm_content END  AS utm_content
    ,sha2(user_custom_id,256)                                                                       AS customer_id
    ,event_id
    ,browser_name
    ,browser_user_agent
    ,click_id
    ,device_is_mobile
    ,device_type
    ,event_timestamp
    ,event_type
    ,geo_country
    ,os_name
    ,page_url
    ,page_url_path
    ,referer_medium
    ,referer_url
    ,user_domain_id                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 
FROM
    lakehouse_federation_bigquery.flavio_malavazi.tab_web_events
