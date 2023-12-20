SELECT
    session_uuid as session_id
    ,utm_source
    ,utm_campaign
    ,utm_content
    ,sha2(user_id,256)                                                                                          AS customer_id
    ,sha2(utm_source || "-" || utm_campaign || "-" || utm_content, 256)                                         AS ad_id
    ,event_id
    ,event_timestamp
    ,page_url
    ,page_url_path
    ,page_url_domain
    ,browser                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
FROM
    lakehouse_federation_bigquery.flavio_malavazi.tab_analytics_events
