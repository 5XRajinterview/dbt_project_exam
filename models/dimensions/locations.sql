with mytable as(
select distinct
Location_ISO_Code,Location,Location_Level,Province,Country,Continent,Island,
Time_Zone,Special_Status,Total_Regencies,Total_Cities,Total_Districts,Total_Urban_Villages,Total_Rural_Villages,Area_km_2_,
Population,Population_Density,Longitude,Latitude,
_FIVETRAN_SYNCED::timestamp,
'{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time

from {{ source('stg','COVID_19_INDONESIA_Adhiraj_Majumdar') }} 
)

select * from mytable

