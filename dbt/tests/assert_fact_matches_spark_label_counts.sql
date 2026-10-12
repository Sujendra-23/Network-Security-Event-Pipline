-- Custom data-quality test: the star schema must reproduce the Spark job's own per-day,
-- per-label flow counts exactly. Any returned row is a day/label where the two disagree
-- (including labels present on only one side).
with dbt_counts as (
    select d.full_date, l.label, count(*) as flow_count
    from {{ ref('fact_flows') }} f
    join {{ ref('dim_date') }} d on d.date_key = f.date_key
    join {{ ref('dim_label') }} l on l.label_key = f.label_key
    group by 1, 2
),

spark_counts as (
    select event_date as full_date, replace(label, chr(65533), '-') as label, sum(flow_count) as flow_count
    from {{ ref('brz_spark_label_counts') }}
    group by 1, 2
)

select
    coalesce(d.full_date, s.full_date) as full_date,
    coalesce(d.label, s.label) as label,
    d.flow_count as dbt_flow_count,
    s.flow_count as spark_flow_count
from dbt_counts d
full outer join spark_counts s on s.full_date = d.full_date and s.label = d.label
where d.flow_count is distinct from s.flow_count
