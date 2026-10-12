-- Bronze: the Spark job's cleaned fact table, read as-is from Delta (no transformation).
-- delta_scan honours the Delta log, so vacuumed or superseded files are not read.
select * from delta_scan('{{ var("delta_dir") }}/network_events')
