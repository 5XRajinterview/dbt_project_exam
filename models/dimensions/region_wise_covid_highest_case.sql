With Locations as (
    select * from {{ ref('Locations') }}
),

Recovered as(
    select * from {{ ref('Recovered') }}
),

deaths as(
    select * from {{ ref('deaths') }}
),

denorm as (
    select Locations.Location_ISO_Code,
    max(NEW_DEATHS) as NEW_DEATHS,max(TOTAL_DEATHS) as TOTAL_DEATHS,max(NEW_DEATHS_PER_MILLION) as NEW_DEATHS_PER_MILLION,max(CASE_FATALITY_RATE) as CASE_FATALITY_RATE,
    max(NEW_RECOVERED) as NEW_RECOVERED,max(TOTAL_RECOVERED) as TOTAL_RECOVERED,max(NEW_ACTIVE_CASES) as NEW_ACTIVE_CASES,
    Location,Longitude,Latitude
from Locations
left join Recovered on Locations.LOCATION_ISO_CODE=Recovered.LOCATION_ISO_CODE
left join deaths on Locations.LOCATION_ISO_CODE=deaths.LOCATION_ISO_CODE
group by Locations.Location_ISO_Code,Location,Longitude,Latitude
)

select 
*,
    '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
 from denorm