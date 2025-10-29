{{ config(materialized='table') }}

with cohort as (
    select
        *
    from {{ ref('int_depression__cohort') }}
),

dx_code as (
    select
        patid,
        dx_date,
        1 as 'has_dx'
    from {{ ref('diagnosis') }}
    where dx = 'F53.0'
),

epds as (
    select
        patid,
        {{ add_time_to_date_macro("obsclin_start_date", "obsclin_start_time") }} obsclin_start_date,
        obsclin_result_num,
        1 as 'has_epds'
    from {{ ref('obs_clin') }}
    where obsclin_code = '99046-5' or obsclin_code = '71354-5'
),

phq9 as (
    select
        patid,
        {{ add_time_to_date_macro("obsclin_start_date", "obsclin_start_time") }} obsclin_start_date,
        obsclin_result_num,
        1 as 'has_phq9'
    from {{ ref('obs_clin') }}
    where obsclin_code = '21012959'
),

all_posibilities as (
    select
        cohort.birthid,
        max(coalesce(dx_code.has_dx, 0)) as dx_code,
        max(coalesce(epds.obsclin_result_num, 0)) as max_epds,
        max(coalesce(epds.has_epds, 0)) as has_epds,
        max(coalesce(phq9.obsclin_result_num, 0)) as max_phq9,
        max(coalesce(phq9.has_phq9, 0)) as has_phq9
    from cohort
    left join dx_code on cohort.mother_patid = dx_code.patid
     and dx_code.dx_date between cohort.baby_birth_date and dateadd(year, 1, cohort.baby_birth_date)
    left join epds on cohort.mother_patid = epds.patid
     and epds.obsclin_start_date between cohort.baby_birth_date and dateadd(year, 1, cohort.baby_birth_date)
    left join phq9 on cohort.mother_patid = phq9.patid
     and phq9.obsclin_start_date between cohort.baby_birth_date and dateadd(year, 1, cohort.baby_birth_date)
    group by cohort.birthid
)

select
    birthid,
    'target_variable' as 'feature_name',
    case when dx_code = 1 or max_epds >= 10 or max_phq9 >= 10 then 1 else 0 end as 'value'
from all_posibilities

union all

select
    birthid,
    'has_screened' as 'feature_name',
    case when has_epds = 1 or has_phq9 = 1 then 1 else 0 end as 'value'
from all_posibilities
