{{
    config(
        database='DBT_INFOFISCUS_LANDING_DEV'
        
    )
}}

WITH CAMPAIGNS as (
SELECT 
"ID","ISDELETED","NAME","PARENTID",NULL AS "RECORDTYPEID","TYPE","STATUS","STARTDATE","ENDDATE",
"EXPECTEDREVENUE","BUDGETEDCOST","ACTUALCOST",NULL AS "CAMPAIGNIMAGEID","EXPECTEDRESPONSE","NUMBERSENT",
"ISACTIVE","DESCRIPTION","NUMBEROFLEADS","NUMBEROFCONVERTEDLEADS","NUMBEROFCONTACTS","NUMBEROFRESPONSES",
"NUMBEROFOPPORTUNITIES","NUMBEROFWONOPPORTUNITIES","AMOUNTALLOPPORTUNITIES","AMOUNTWONOPPORTUNITIES",
NULL AS "CURRENCYISOCODE","OWNERID", NULL AS "TENANTID","CREATEDDATE","CREATEDBYID","LASTMODIFIEDDATE",
"LASTMODIFIEDBYID","SYSTEMMODSTAMP","LASTACTIVITYDATE","LASTVIEWEDDATE","LASTREFERENCEDDATE",
"CAMPAIGNMEMBERRECORDTYPEID",
  CURRENT_TIMESTAMP AS INSERT_DT
,"ID" AS KEY_ID
FROM
{{source('Infofiscus','CAMPAIGNS')}}
INNER JOIN DBT_INFOFISCUS_LANDING_DEV.CT_SALESFORCE.INFOFISCUS_CT
ON SRC_OBJECT ='CAMPAIGNS' AND TGT_OBJECT = 'STG_CAMPAIGNS' 
 WHERE LASTMODIFIEDDATE >= LAST_RUN_DATE
)

SELECT * FROM CAMPAIGNS
            