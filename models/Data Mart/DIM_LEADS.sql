{{
    config(
        materialized = 'incremental',
        unique_key='ID',
        incremental_strategy='merge',
        merge_update_columns = ["ID","ISDELETED","MASTERRECORDID","LASTNAME","FIRSTNAME","SALUTATION","NAME","TITLE","COMPANY","STREET",
"CITY","STATE","POSTALCODE","COUNTRY","LATITUDE","LONGITUDE","GEOCODEACCURACY","PHONE","MOBILEPHONE","FAX","EMAIL","WEBSITE","PHOTOURL",
"DESCRIPTION","LEADSOURCE","STATUS","INDUSTRY","RATING","ANNUALREVENUE","NUMBEROFEMPLOYEES","OWNERID","ISCONVERTED","CONVERTEDDATE",
"CONVERTEDACCOUNTID","CONVERTEDCONTACTID","CONVERTEDOPPORTUNITYID","ISUNREADBYOWNER","CREATEDDATE","CREATEDBYID","LASTMODIFIEDDATE",
"LASTMODIFIEDBYID","SYSTEMMODSTAMP","LASTACTIVITYDATE","LASTVIEWEDDATE","LASTREFERENCEDDATE","JIGSAW","JIGSAWCONTACTID",
"CLEANSTATUS","COMPANYDUNSNUMBER","DANDBCOMPANYID","EMAILBOUNCEDREASON","EMAILBOUNCEDDATE","INDIVIDUALID",  "DW_UPDATE_DT"],
        database='DBT_INFOFISCUS_DATAMART_DEV', 
        post_hook="Update DBT_INFOFISCUS_LANDING_DEV.CT_SALESFORCE.INFOFISCUS_CT CT SET LAST_RUN_DATE = CURRENT_TIMESTAMP() WHERE
        SRC_OBJECT =  'LEADS' AND TGT_OBJECT = 'STG_LEADS' ;"
    )
}}

SELECT 
"ID",
"ISDELETED",
"MASTERRECORDID",
"LASTNAME",
"FIRSTNAME",
"SALUTATION",
"NAME",
"TITLE",
"COMPANY",
"STREET",
"CITY",
"STATE",
"POSTALCODE",
"COUNTRY",
"LATITUDE",
"LONGITUDE",
"GEOCODEACCURACY",
"PHONE",
"MOBILEPHONE",
"FAX",
"EMAIL",
"WEBSITE",
"PHOTOURL",
"DESCRIPTION",
"LEADSOURCE",
"STATUS",
"INDUSTRY",
"RATING",
"ANNUALREVENUE",
"NUMBEROFEMPLOYEES",
"OWNERID",
"ISCONVERTED",
"CONVERTEDDATE",
"CONVERTEDACCOUNTID",
"CONVERTEDCONTACTID",
"CONVERTEDOPPORTUNITYID",
"ISUNREADBYOWNER",
"CREATEDDATE",
"CREATEDBYID",
"LASTMODIFIEDDATE",
"LASTMODIFIEDBYID",
"SYSTEMMODSTAMP",
"LASTACTIVITYDATE",
"LASTVIEWEDDATE",
"LASTREFERENCEDDATE",
"JIGSAW",
"JIGSAWCONTACTID",
"CLEANSTATUS",
"COMPANYDUNSNUMBER",
"DANDBCOMPANYID",
"EMAILBOUNCEDREASON",
"EMAILBOUNCEDDATE",
"INDIVIDUALID", "ID" AS "DW_KEY_ID",     
CURRENT_TIMESTAMP AS "DW_INSERT_DT", CURRENT_TIMESTAMP AS "DW_UPDATE_DT" 
FROM {{ref('STG_LEADS')}}
            