with mytable as(
select 
date,Location_ISO_Code,NEW_RECOVERED,TOTAL_RECOVERED,NEW_ACTIVE_CASES,CASE_RECOVERED_RATE,
_FIVETRAN_SYNCED::timestamp,
'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time

from {{ source('stg','COVID_19_INDONESIA_Adhiraj_Majumdar') }} 
)

select * from mytable