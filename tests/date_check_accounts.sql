SELECT * 
FROM {{ref('DIM_ACCOUNTS')}}
WHERE CREATEDDATE>LASTMODIFIEDDATE
