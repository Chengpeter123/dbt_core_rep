{{
    config(
        materialized = 'incremental',
        unique_key='ID',
        incremental_strategy='merge',
        merge_update_columns = ["ID","ISDELETED","ACCOUNTID","ISPRIVATE","NAME","DESCRIPTION","STAGENAME","AMOUNT","PROBABILITY",
"EXPECTEDREVENUE","TOTALOPPORTUNITYQUANTITY","CLOSEDATE","TYPE","NEXTSTEP","LEADSOURCE","ISCLOSED","ISWON","FORECASTCATEGORY",
"FORECASTCATEGORYNAME","CAMPAIGNID","HASOPPORTUNITYLINEITEM","PRICEBOOK2ID","OWNERID","CREATEDDATE","CREATEDBYID","LASTMODIFIEDDATE",
"LASTMODIFIEDBYID","SYSTEMMODSTAMP","LASTACTIVITYDATE","PUSHCOUNT","LASTSTAGECHANGEDATE","FISCALQUARTER","FISCALYEAR","FISCAL","CONTACTID","LASTVIEWEDDATE",
"LASTREFERENCEDDATE","HASOPENACTIVITY","HASOVERDUETASK","LASTAMOUNTCHANGEDHISTORYID","LASTCLOSEDATECHANGEDHISTORYID", "DW_UPDATE_DT"],
        database='DBT_INFOFISCUS_DATAMART_DEV', 
        post_hook="Update DBT_INFOFISCUS_LANDING_DEV.CT_SALESFORCE.INFOFISCUS_CT CT SET LAST_RUN_DATE = CURRENT_TIMESTAMP() WHERE
        SRC_OBJECT =  'OPPORTUNITIES' AND TGT_OBJECT = 'STG_OPPORTUNITIES' ;"
    )
}}

SELECT 
"ID",
"ISDELETED",
"ACCOUNTID",
"ISPRIVATE",
"NAME",
"DESCRIPTION",
"STAGENAME",
"AMOUNT",
"PROBABILITY",
"EXPECTEDREVENUE",
"TOTALOPPORTUNITYQUANTITY",
"CLOSEDATE",
"TYPE",
"NEXTSTEP",
"LEADSOURCE",
"ISCLOSED",
"ISWON",
"FORECASTCATEGORY",
"FORECASTCATEGORYNAME",
"CAMPAIGNID",
"HASOPPORTUNITYLINEITEM",
"PRICEBOOK2ID",
"OWNERID",
"CREATEDDATE",
"CREATEDBYID",
"LASTMODIFIEDDATE",
"LASTMODIFIEDBYID",
"SYSTEMMODSTAMP",
"LASTACTIVITYDATE",
"PUSHCOUNT",
"LASTSTAGECHANGEDATE",
"FISCALQUARTER",
"FISCALYEAR",
"FISCAL",
"CONTACTID",
"LASTVIEWEDDATE",
"LASTREFERENCEDDATE",
"HASOPENACTIVITY",
"HASOVERDUETASK",
"LASTAMOUNTCHANGEDHISTORYID",
"LASTCLOSEDATECHANGEDHISTORYID", "ID" AS "DW_KEY_ID",     
CURRENT_TIMESTAMP AS "DW_INSERT_DT", CURRENT_TIMESTAMP AS "DW_UPDATE_DT" 
FROM {{ref('STG_OPPORTUNITIES')}}
            