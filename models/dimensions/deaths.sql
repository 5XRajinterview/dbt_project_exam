with mytable as(
select 
date,Location_ISO_Code,NEW_DEATHS,TOTAL_DEATHS,NEW_DEATHS_PER_MILLION,CASE_FATALITY_RATE,TOTAL_CASES,GROWTH_FACTOR_OF_NEW_DEATHS,
_FIVETRAN_SYNCED::timestamp,
'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time

from {{ source('stg','COVID_19_INDONESIA_Adhiraj_Majumdar') }} 
)

select * from mytable
