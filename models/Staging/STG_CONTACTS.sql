{{ 
        config(
        materialized="table",
        enabled=true,
        database= 'DBT_INFOFISCUS_STAGING_DEV'
    
        ) 
}}

    select * from {{ref('VW_CONTACTS')}};