{{
    config(
        materialized = 'incremental',
        unique_key='ID',
        incremental_strategy='merge',
        merge_update_columns = ["ID","ISDELETED","NAME","PARENTID",NULL AS "RECORDTYPEID","TYPE","STATUS","STARTDATE","ENDDATE",
"EXPECTEDREVENUE","BUDGETEDCOST","ACTUALCOST",NULL AS "CAMPAIGNIMAGEID","EXPECTEDRESPONSE","NUMBERSENT",
"ISACTIVE","DESCRIPTION","NUMBEROFLEADS","NUMBEROFCONVERTEDLEADS","NUMBEROFCONTACTS","NUMBEROFRESPONSES",
"NUMBEROFOPPORTUNITIES","NUMBEROFWONOPPORTUNITIES","AMOUNTALLOPPORTUNITIES","AMOUNTWONOPPORTUNITIES",
NULL AS "CURRENCYISOCODE","OWNERID", NULL AS "TENANTID","CREATEDDATE","CREATEDBYID","LASTMODIFIEDDATE",
"LASTMODIFIEDBYID","SYSTEMMODSTAMP","LASTACTIVITYDATE","LASTVIEWEDDATE","LASTREFERENCEDDATE",
"CAMPAIGNMEMBERRECORDTYPEID", "DW_UPDATE_DT"],
        database='DBT_INFOFISCUS_DATAMART_DEV', 
        post_hook="Update DBT_INFOFISCUS_LANDING_DEV.CT_SALESFORCE.INFOFISCUS_CT CT SET LAST_RUN_DATE = CURRENT_TIMESTAMP() WHERE
        SRC_OBJECT =  'CAMPAIGNS' AND TGT_OBJECT = 'STG_CAMPAIGNS' ;"
    )
}}

SELECT 
"ID","ISDELETED","NAME","PARENTID",NULL AS "RECORDTYPEID","TYPE","STATUS","STARTDATE","ENDDATE",
"EXPECTEDREVENUE","BUDGETEDCOST","ACTUALCOST",NULL AS "CAMPAIGNIMAGEID","EXPECTEDRESPONSE","NUMBERSENT",
"ISACTIVE","DESCRIPTION","NUMBEROFLEADS","NUMBEROFCONVERTEDLEADS","NUMBEROFCONTACTS","NUMBEROFRESPONSES",
"NUMBEROFOPPORTUNITIES","NUMBEROFWONOPPORTUNITIES","AMOUNTALLOPPORTUNITIES","AMOUNTWONOPPORTUNITIES",
NULL AS "CURRENCYISOCODE","OWNERID", NULL AS "TENANTID","CREATEDDATE","CREATEDBYID","LASTMODIFIEDDATE",
"LASTMODIFIEDBYID","SYSTEMMODSTAMP","LASTACTIVITYDATE","LASTVIEWEDDATE","LASTREFERENCEDDATE",
"CAMPAIGNMEMBERRECORDTYPEID", "ID" AS "DW_KEY_ID",     
CURRENT_TIMESTAMP AS "DW_INSERT_DT", CURRENT_TIMESTAMP AS "DW_UPDATE_DT" 
FROM {{ref('STG_CAMPAIGNS')}}
            