{{
    config(
        database='DBT_INFOFISCUS_LANDING_DEV'
        
    )
}}

WITH LEADS as (
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
"INDIVIDUALID",
  CURRENT_TIMESTAMP AS INSERT_DT
,"ID" AS KEY_ID
FROM
{{source('Infofiscus','LEADS')}}
INNER JOIN DBT_INFOFISCUS_LANDING_DEV.CT_SALESFORCE.INFOFISCUS_CT
ON SRC_OBJECT ='LEADS' AND TGT_OBJECT = 'STG_LEADS' 
 WHERE LASTMODIFIEDDATE >= LAST_RUN_DATE
)

SELECT * FROM LEADS
            