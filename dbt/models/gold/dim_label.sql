-- Gold dimension: one row per label, enriched from the CURRENT version of the SCD2 snapshot
-- (history stays queryable in snapshots.snap_label_catalog). Labels seen in the traffic but
-- absent from the catalog are kept as 'Unclassified' so every fact row has a dimension row.
with seen as (
    select distinct label from {{ ref('slv_flows') }}
),

catalog as (
    select label, attack_family, severity
    from {{ ref('snap_label_catalog') }}
    where dbt_valid_to is null
),

all_labels as (
    select label from seen
    union
    select label from catalog
)

select
    md5(l.label) as label_key,
    l.label,
    coalesce(c.attack_family, 'Unclassified') as attack_family,
    coalesce(c.severity, 'unknown') as severity,
    upper(l.label) <> 'BENIGN' as is_attack,
    c.label is null as is_unclassified
from all_labels l
left join catalog c on c.label = l.label
