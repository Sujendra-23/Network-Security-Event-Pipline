-- Silver: one typed, cleaned row per flow, with a deterministic surrogate key.
--
-- The dataset has no flow id, and CICFlowMeter exports contain exact duplicate rows (about 10%
-- of the full dataset), which are separate observations rather than errors. flow_key is therefore
-- the md5 of the whole row (minus the machine-specific input path) plus an occurrence number
-- among identical rows. Identical rows are interchangeable, so the set of keys is stable
-- across runs and machines.
with src as (
    select
        * exclude (_source_file),
        regexp_extract(_source_file, '[^/]+$') as source_file
    from {{ ref('brz_network_events') }}
),

hashed as (
    select src.*, md5(cast(src as varchar)) as row_hash
    from src
)

select
    row_hash || '-' || cast(row_number() over (partition by row_hash) as varchar) as flow_key,
    event_date,
    event_time,
    attack_category,
    label as label_raw,
    -- Some mirrors store the en dash in "Web Attack – Brute Force" as invalid UTF-8; it decodes
    -- to U+FFFD. Normalise to an ASCII hyphen so the label is joinable and readable.
    replace(label, chr(65533), '-') as label,
    source_file,
    {{ optional_column(ref('brz_network_events'), 'Destination_Port', 'integer') }} as destination_port,
    {{ optional_column(ref('brz_network_events'), 'Flow_Duration', 'bigint') }} as flow_duration_us,
    {{ optional_column(ref('brz_network_events'), 'Total_Fwd_Packets', 'bigint') }} as total_fwd_packets,
    {{ optional_column(ref('brz_network_events'), 'Total_Backward_Packets', 'bigint') }} as total_bwd_packets,
    {{ optional_column(ref('brz_network_events'), 'Total_Length_of_Fwd_Packets', 'bigint') }} as total_fwd_bytes,
    {{ optional_column(ref('brz_network_events'), 'Total_Length_of_Bwd_Packets', 'bigint') }} as total_bwd_bytes,
    {{ optional_column(ref('brz_network_events'), 'Flow_Bytes/s', 'double') }} as flow_bytes_per_sec,
    {{ optional_column(ref('brz_network_events'), 'Flow_Packets/s', 'double') }} as flow_packets_per_sec,
    coalesce({{ optional_column(ref('brz_network_events'), 'Flow_Duration', 'bigint') }} >= 0, true)
        as is_duration_valid
from hashed
