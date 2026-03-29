select *
from {{ ref('fct_trips') }}
where total_amount < fare_amount
