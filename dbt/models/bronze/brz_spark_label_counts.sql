-- Bronze: the Spark job's own per-day label aggregate. Used only to reconcile the dbt star
-- schema against an independently computed result (tests/assert_fact_matches_spark_label_counts).
select * from delta_scan('{{ var("delta_dir") }}/label_counts')
