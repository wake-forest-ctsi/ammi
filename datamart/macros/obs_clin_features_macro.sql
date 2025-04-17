{% macro obs_clin_features_macro(date1, date2) %}

-- get these features:
{% set features = ('8867-4','8478-0','9279-1','20564-1','8310-5') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

obs_clin as (
    select
        patid,
        obsclin_code,
        obsclin_result_num,
        obsclin_start_date
    from {{ ref('obs_clin') }}
    where obsclin_type = 'LC'
      and obsclin_result_modifier = 'EQ'
      and obsclin_code  in {{ features }}
      
),

obs_clin_stats as (
    select
        distinct  -- remember the distinct here!
        cohort.birthid,
        obs_clin.obsclin_code,
        min(obsclin_result_num) over (partition by birthid, obsclin_code) as 'value_min',
        max(obsclin_result_num) over (partition by birthid, obsclin_code) as 'value_max',
        avg(obsclin_result_num) over (partition by birthid, obsclin_code) as 'value_mean',
        percentile_cont(0.5) within group (order by obsclin_result_num) over (partition by birthid, obsclin_code) as 'value_median'
    from cohort
    left join obs_clin on cohort.mother_patid = obs_clin.patid
     and obsclin_start_date between {{ date1 }} and {{ date2 }}
),

renamed as (
    select
        birthid,
        {% for feature in features %}
            sum(case when obsclin_code = '{{ feature }}' then value_min else null end) as 'LC_{{feature}}_min',
            sum(case when obsclin_code = '{{ feature }}' then value_max else null end) as 'LC_{{feature}}_max',
            sum(case when obsclin_code = '{{ feature }}' then value_mean else null end) as 'LC_{{feature}}_mean',
            sum(case when obsclin_code = '{{ feature }}' then value_median else null end) as 'LC_{{feature}}_median' {% if not loop.last %},{% endif %}
        {% endfor %}
    from obs_clin_stats
    group by birthid
)


select * from renamed

{% endmacro %}