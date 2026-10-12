{#- Custom data-quality test: returns the rows where a numeric column is negative. -#}
{% test non_negative(model, column_name) %}
select {{ column_name }}
from {{ model }}
where {{ column_name }} < 0
{% endtest %}
