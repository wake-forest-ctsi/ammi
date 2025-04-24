{% macro add_time_to_date_macro(date, time) %}
cast({{ date }} as datetime) + cast({{ time }} as datetime)
{% endmacro %}