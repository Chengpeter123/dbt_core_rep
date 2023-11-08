import os
import snowflake.connector
import pandas as pd
import numpy as np


# Define the project name and models directory
project_name = "my_dbt_project"
models_dir = "Landing"
models_dir2 = "Staging"
models_dir3 = "Data Mart"


conn = snowflake.connector.connect(
  user='Siddharth',
  password='ac2F!T#r67aHt8lmn1l1',
  account='ixa93060',
  warehouse='COMPUTE_WH',
  database='DBT_INFOFISCUS_LANDING_DEV',
  schema='CT_SALESFORCE'
)
cur = conn.cursor()
cur.execute('SELECT * FROM INFOFISCUS_CT')

df = cur.fetchall()
data = pd.DataFrame(df, columns = ["ROW_NO",  ' ENVIRONMENT ', 'SRC_DB', 'SRC_SCHEMA', 'SRC_OBJECT',
        'PRIMARY_KEY', 'COLUMN_NAMES', 'TGT_DB', 'TGT_SCHEMA', 'TGT_OBJECT', 'LAST_RUN_TIMESTAMP'
])
#print(data)

dictionary = data.to_dict(orient='records')

#print(dictionary)

# Create the project directory
os.makedirs(project_name, exist_ok=True)

# Create the dbt_project.yml file
dbt_project_yml = f"""
name: '{project_name}'
version: '1.0'
profile: 'default'
config-version: 2
"""

with open(os.path.join(project_name, 'dbt_project.yml'), 'w') as f:
    f.write(dbt_project_yml)

# Create the profiles.yml file (you may need to customize this based on your dbt profile)
profiles_yml = """
version: 2

sources:
  - name: Infofiscus
    database: DBT_INFOFISCUS_LANDING_DEV
    schema: PUBLIC   
    tables: 
      - name: ACCOUNT
      - name: CAMPAIGN 
      - name: CONTACT 
      - name: OPPORTUNITY
"""

with open(os.path.join(project_name, 'profiles.yml'), 'w') as f:
    f.write(profiles_yml)

# Create the models directory
os.makedirs(os.path.join(project_name, models_dir), exist_ok=True)

# Define a list of models
models = ("VW_ACCOUNTS", "VW_CAMPAIGNS", "VW_CONTACTS")

source_list = [d['SRC_OBJECT'] for d in dictionary]
source_schema_list = [d['SRC_SCHEMA'] for d in dictionary]
source_database_list = [d['SRC_DB'] for d in dictionary]
primarykey_list = [d['PRIMARY_KEY'] for d in dictionary]
COLUMN_NAMES_list = [d['COLUMN_NAMES'] for d in dictionary]
TGT_OBJECT_list = [d['TGT_OBJECT'] for d in dictionary]
TGT_database_list = [d['TGT_DB'] for d in dictionary]



result = " ".join(COLUMN_NAMES_list)

result2=result.split(',')
concated_items=''
for i in result2:
  concated_items+=('"'+i+'"'+',')
# print(concated_items)
print("result2", result2)

print(COLUMN_NAMES_list)
i = 0
# Create model files
for model in models:
    model_content = \
f"""{{{{
    config(
        database='{source_database_list[i]}'
        
    )
}}}}

WITH {source_list[i]} as (
SELECT 
{COLUMN_NAMES_list[i]}
  CURRENT_TIMESTAMP AS INSERT_DT
,"{primarykey_list[i]}" AS KEY_ID
FROM
{{{{source('Infofiscus','{source_list[i]}')}}}}
INNER JOIN INFOFISCUS_LANDING_DEV.CT_SALESFORCE.INFOFISCUS_CT
ON SRC_TABLE = {source_list[i]} AND TGT_TABLE = {TGT_OBJECT_list[i]} 
 WHERE DATE_LAST_MODIFIED >= LAST_RUN_DATE
)

SELECT * FROM {source_list[i]}
            """



    with open(os.path.join(project_name, models_dir, f'{model}.sql'), 'w') as f:
        f.write(model_content)

    i = i + 1

staging =("STG_ACCOUNTS", "STG_CAMPAIGNS", "STG_CONTACTS")

z = 0
for stage in staging:
    stage_content = \
f"""{{{{ 
        config(
        materialized="table",
        enabled=true,
        database= '{TGT_database_list[z]}'
    
        ) 
}}}}

    select * from {{{{ref('VW_{source_list[z]}')}}}};"""

    with open(os.path.join(project_name, models_dir2, f'{stage}.sql'), 'w') as f:
        f.write(stage_content)
    z = z+1

mart = ("DIM_ACCOUNTS","DIM_CAMPAIGNS", "DIM_CONTACTS")


y = 0
for marts in mart:

    mart_content = \
f"""{{{{
    config(
        materialized = 'incremental',
        unique_key='{primarykey_list[y]}',
        incremental_strategy='merge',
        merge_update_columns = [{COLUMN_NAMES_list[y]} "DW_UPDATE_DT"],
        database="INFOFISCUS_DATAMART_DEV",
        post_hook="Update INFOFISCUS_LANDING_QA.PUBLIC.INFOFISCUS_CT CT SET LAST_RUN_DATE = CURRENT_TIMESTAMP() WHERE
SRC_VIEW = 'VW_<SRC_TABLE>' AND TGT_TABLE = '<TGT_TABLE>' AND ENVIRONMENT = 'DATA_MART';"
    )
}}}}

SELECT 
{COLUMN_NAMES_list[y]} "{primarykey_list[y]}" AS "DW_KEY_ID",     
CURRENT_TIMESTAMP AS "DW_INSERT_DT", CURRENT_TIMESTAMP AS "DW_UPDATE_DT" 
FROM {{{{ref('{TGT_OBJECT_list[y]}')}}}}
            """
    with open(os.path.join(project_name, models_dir3, f'{marts}.sql'), 'w') as f:
        f.write(mart_content)
    y = y+1


print("dbt project files and models created successfully.")


cur.close()
conn.close()