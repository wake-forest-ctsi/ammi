{% set features = ( '13011', '21012976', '21012977',
      '21012954', '21012949', '21012951', '21012960', '21012948', 
      '21012956', '21012953', '21012958', '21012955', '21012950',
      '99046-5', '71354-5', '21012959') %}

with cohort as (
    select
        birthid,
        mother_patid,
        estimated_preg_start_date,
        baby_birth_date,
        delivery_admit_date,
        delivery_discharge_date
    from {{ ref('int_cohort') }}
),

phq_epds as (
    select
        patid,
        {{ add_time_to_date_macro("obsclin_start_date", "obsclin_start_time") }} obsclin_start_date,
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

renamed as (
    select
        a.birthid,
        a.mother_patid,
        a.baby_birth_date,
        a.delivery_admit_date,
        a.delivery_discharge_date,
        b.obsclin_start_date,
        datediff(day, a.estimated_preg_start_date, b.obsclin_start_date) as gestage_days,
        b.obsclin_code,
        b.phq_value
    from cohort a
    left join phq_epds b on a.mother_patid = b.patid
)

select * from renamed