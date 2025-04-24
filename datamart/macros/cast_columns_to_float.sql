{% macro cast_columns_to_float(table_name) %}
    {{ print(table_name) }}
    {% set cols = adapter.get_columns_in_relation(ref(table_name)) %}
    {% set outputs = [] %}
    {% for col in cols %}
        {% do outputs.append('cast (' ~ col.name ~ ' as float) as ' ~ col.name ~ ',') %}
    {% endfor %}
    {% set output_final = outputs | join ('\n') %}
    {{ print(output_final) }}
{% endmacro %}