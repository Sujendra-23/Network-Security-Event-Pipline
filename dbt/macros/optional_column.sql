{#-
  Select a column of `relation` cast to `dtype`, or a typed NULL when this mirror of the dataset
  does not have it. jobs/schema.py degrades the same way for optional CIC-IDS2017 columns: the
  Kaggle mirror has no IP/timestamp columns, while the CI fixture has only a handful of features.
-#}
{% macro optional_column(relation, column, dtype) -%}
    {%- if execute -%}
        {%- set present = adapter.get_columns_in_relation(relation) | map(attribute='name') | map('lower') | list -%}
    {%- else -%}
        {%- set present = [] -%}
    {%- endif -%}
    {%- if column | lower in present -%}
        try_cast("{{ column }}" as {{ dtype }})
    {%- else -%}
        cast(null as {{ dtype }})
    {%- endif -%}
{%- endmacro %}
