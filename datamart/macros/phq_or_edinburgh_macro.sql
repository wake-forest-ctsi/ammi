-- see excel file for details

{% macro phq_or_edinburgh_macro(date1, date2) %}

{% set features = ( '13011', '21012976', '21012977',
      '21012954', '21012949', '21012951', '21012960', '21012948', 
      '21012956', '21012953', '21012958', '21012955', '21012950',
      '99046-5', '71354-5', '21012959') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

phq as (
    select
        patid,
        obsclin_start_date,
        obsclin_code,
        case 
            when obsclin_code in ('21012954', '21012949', '21012951', '21012948', '21012956', 
                                  '21012953', '21012958', '21012955', '21012950') then
                case when obsclin_result_text = 'Not at all' then 0
                     when obsclin_result_text = 'Several days' then 1
                     when obsclin_result_text = 'More than half the days' then 2
                     when obsclin_result_text = 'Nearly every day' then 3
                     when obsclin_result_text = 'No information' then NULL
                     else -99 end
            when obsclin_code in ('21012960') then
                case when obsclin_result_text = 'Not difficult at all' then 0
                     when obsclin_result_text = 'Somewhat difficult' then 1
                     when obsclin_result_text = 'Very difficult' then 2
                     when obsclin_result_text = 'Extremely difficult' then 3
                     when obsclin_result_text = 'No information' then NULL
                     else -99 end
            when obsclin_code in ('21012977', '21012976') then
                case when obsclin_result_text = 'No' then 0
                     when obsclin_result_text = 'Yes' then 1
                     when obsclin_result_text = 'No information' then NULL
                     else -99 end
            when obsclin_code in ('13011') then
                case when obsclin_result_text = 'Patient lacks the functional capacity to participa' then 3
                     when obsclin_result_text = 'Patient Refusal' then 2
                     when obsclin_result_text = 'PHQ9 Preferred' then 1
                     else 0 end
            when obsclin_code in ('99046-5', '71354-5', '21012959') then
                obsclin_result_num
            else
                -99
        end as phq_value
    from {{ ref('obs_clin') }}
    where obsclin_code is not null
      and obsclin_code in {{ features }} 
),

phq_stats as (
    select
        cohort.birthid,
        obsclin_code,
        count(phq_value) as value_counts,
        min(phq_value) as value_min,
        max(phq_value) as value_max,
        avg(phq_value) as value_mean
    from cohort
    left join phq on cohort.mother_patid = phq.patid
     and obsclin_start_date between {{ date1 }} and {{ date2 }}
    group by cohort.birthid, obsclin_code
),

renamed as (
    select
        birthid,
        {% for feature in features %}
            sum(case when obsclin_code = '{{ feature }}' then value_max else null end) as 'phq_or_edinburgh_{{ feature.split('-')[0] }}_max' {% if not loop.last %},{% endif %}
        {% endfor %}
    from phq_stats
    group by birthid
)

select * from renamed

{% endmacro %}