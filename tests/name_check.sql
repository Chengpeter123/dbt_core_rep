SELECT COUNT(*)
FROM {{ref('DIM_ACCOUNTS')}}
WHERE (FIRSTNAME||' '||LASTNAME) <> NAME
