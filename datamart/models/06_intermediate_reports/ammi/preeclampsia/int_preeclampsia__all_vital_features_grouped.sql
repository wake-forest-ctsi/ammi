{{ config(materialized='table') }}

{% set features = ['wt', 'systolic', 'diastolic', 'computed_map', 'pulse_pressure', 'original_bmi', 'computed_bmi'] %}

with median_cte as (
    {% for f in features %}
        select
            distinct
            birthid,
            '{{ f }}_median' as feature_name,
            percentile_cont(0.5) within group (order by {{ f }}) over (partition by birthid) as 'value'
        from {{ ref('int_preeclampsia__all_vital_features') }}
    {% if not loop.last %} union all {% endif %}
    {% endfor %}
),

other_stat_cte as (
    select
        birthid,
        {% for f in features %}
            avg({{ f }}) as {{ f }}_mean_value,
            min({{ f }}) as {{ f }}_min_value,
            max({{ f }}) as {{ f }}_max_value,
            count(1) as {{ f }}_count_value {% if not loop.last %},{% endif %}
        {% endfor %}
    from {{ ref('int_preeclampsia__all_vital_features') }}
    group by birthid
)

select * from median_cte

union all

{% for f in features %}
select
    birthid,
    '{{ f }}_mean_value' as 'feature_name',
    {{ f }}_mean_value as 'value'
from other_stat_cte
union all
select
    birthid,
    '{{ f }}_min_value' as 'feature_name',
    {{ f }}_min_value as 'value'
from other_stat_cte
union all
select
    birthid,
    '{{ f }}_max_value' as 'feature_name',
    {{ f }}_max_value as 'value'
from other_stat_cte
union all
select
    birthid,
    '{{ f }}_count_value' as 'feature_name',
    {{ f }}_count_value as 'value'
from other_stat_cte

{% if not loop.last %} union all {% endif %}
{% endfor %}