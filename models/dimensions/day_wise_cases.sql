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
    select deaths.Date::datetime,
    sum(NEW_DEATHS),sum(TOTAL_DEATHS), sum(NEW_DEATHS_PER_MILLION),sum(CASE_FATALITY_RATE),
     sum(NEW_RECOVERED),sum(TOTAL_RECOVERED),sum(NEW_ACTIVE_CASES)

from Locations
left join Recovered on Locations.LOCATION_ISO_CODE=Recovered.LOCATION_ISO_CODE
left join deaths on Locations.LOCATION_ISO_CODE=deaths.LOCATION_ISO_CODE
group by deaths.Date
)

select 
*,
    '{{ run_started_at.strftime ("%Y-%m-%d %H:%M:%S")}}'::timestamp as dbt_time
 from denorm