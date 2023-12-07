{{ config(materialized='table') }}


select * from {{ ref('vw_bing_ads') }}
union all
select * from {{ ref('vw_facebook_ads') }}
union all
select * from {{ ref('vw_google_ads') }}
union all
select * from {{ ref('vw_mailchimp') }}


/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
