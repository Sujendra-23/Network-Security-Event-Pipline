-- Gold fact: one row per flow (grain = flow_key), keyed to dim_date and dim_label.
select
    flow_key,
    cast(strftime(event_date, '%Y%m%d') as integer) as date_key,
    md5(label) as label_key,
    destination_port,
    source_file,
    flow_duration_us,
    total_fwd_packets,
    total_bwd_packets,
    total_fwd_bytes + total_bwd_bytes as total_bytes,
    flow_bytes_per_sec,
    flow_packets_per_sec,
    attack_category = 'ATTACK' as is_attack,
    is_duration_valid
from {{ ref('slv_flows') }}
