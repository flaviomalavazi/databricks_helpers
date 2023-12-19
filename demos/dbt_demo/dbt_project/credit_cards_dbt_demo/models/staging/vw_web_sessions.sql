SELECT
    session_uuid as session_id
    ,utm_source
    ,case when (utm_source = 'organic' or page_url not ilike '%?utm_source=%') THEN NULL ELSE utm_campaign END  AS utm_campaign
    ,case when (utm_source = 'organic' or page_url not ilike '%?utm_source=%') THEN NULL ELSE utm_content END   AS utm_content
    ,sha2(user_id,256)                                                                                          AS customer_id
    ,CASE
        WHEN page_url ilike '%?utm_source=%' AND utm_source != 'organic' THEN sha2(utm_source || "-" || utm_campaign || "-" || utm_content, 256)
        ELSE NULL
        END                                                                                                     AS ad_id
    ,event_id
    ,event_timestamp
    ,page_url
    ,page_url_path
    ,page_url_domain
    ,browser                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        
FROM
    lakehouse_federation_bigquery.flavio_malavazi.tab_analytics_events
