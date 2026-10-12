-- Gold dimension: a contiguous calendar from the first to the last capture day.
with bounds as (
    select min(event_date) as first_day, max(event_date) as last_day from {{ ref('slv_flows') }}
),

spine as (
    select cast(day as date) as full_date
    from bounds, generate_series(first_day, last_day, interval 1 day) as t(day)
)

select
    cast(strftime(full_date, '%Y%m%d') as integer) as date_key,
    full_date,
    dayname(full_date) as day_name,
    isodow(full_date) as iso_day_of_week,
    isodow(full_date) >= 6 as is_weekend,
    weekofyear(full_date) as iso_week,
    month(full_date) as month,
    year(full_date) as year
from spine
