{# use information in the seed file to generate the date range for different reports #}
{% macro get_date_range(model_name) %}
    {# print("here" ~ model_name) #}
    {% if execute %}
        {% set tmpquery %}
            select 
                top 1
                start_date,
                end_date,
                extra,
                case when model = '{{ model_name }}' then 2
                     when model = 'int_cohort' then 1 -- default to int_cohort
                     else 0 end as ranking
            from {{ ref('daterange') }} where report = '{{ var("report") }}'
            order by ranking desc
        {% endset %}
        {% set date_range_list = run_query(tmpquery).rows[0].values() %}
    {% else %}
        {% set date_range_list = ['1', '2', '3'] %}
    {% endif %}
    {{ return(date_range_list) }}
{% endmacro %}