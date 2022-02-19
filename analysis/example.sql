select *
from {{ ref('dim_date') }}
where id = 1
