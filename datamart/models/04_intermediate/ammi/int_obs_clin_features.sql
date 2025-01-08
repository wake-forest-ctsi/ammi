-- depends_on: {{ ref('daterange') }}

{% set features = ('Pulse Heart Rate', 'MAP Mean Blood Pressure', 'RespiratoryRate', 'SPO2 Oxygen Saturation', 'Body Temperature') %}
{% set date_range_list = get_date_range('int_obs_clin_features') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

obs_clin as (
    select
        patid,
        raw_obsclin_name,
        obsclin_result_num,
        obsclin_start_date
    from {{ ref('stg_pcornet__obs_clin') }}
    where raw_obsclin_name in {{ features }}
),

obs_clin_stats as (
    select
        distinct  -- remember the distinct here!
        cohort.birthid,
        obs_clin.raw_obsclin_name,
        min(obsclin_result_num) over (partition by birthid, raw_obsclin_name) as 'min_value',
        max(obsclin_result_num) over (partition by birthid, raw_obsclin_name) as 'max_value',
        avg(obsclin_result_num) over (partition by birthid, raw_obsclin_name) as 'mean_value',
        percentile_cont(0.5) within group (order by obsclin_result_num) over (partition by birthid, raw_obsclin_name) as 'median_value'
    from cohort
    left join obs_clin on cohort.mother_patid = obs_clin.patid
     and obsclin_start_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
),

renamed as (
    select
        birthid,
        {% for feature in features %}
            sum(case when raw_obsclin_name = '{{ feature }}' then min_value else null end) as 'min_{{ feature}}',
            sum(case when raw_obsclin_name = '{{ feature }}' then max_value else null end) as 'max_{{ feature}}',
            sum(case when raw_obsclin_name = '{{ feature }}' then mean_value else null end) as 'mean_{{ feature}}',
            sum(case when raw_obsclin_name = '{{ feature }}' then median_value else null end) as 'median_{{ feature}}' {% if not loop.last %},{% endif %}
        {% endfor %}
    from obs_clin_stats
    group by birthid
)


select * from renamed