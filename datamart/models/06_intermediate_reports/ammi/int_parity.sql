with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

obs_clin as (
    select
        patid,
        obsclin_result_num as parity,
        {{ add_time_to_date_macro("obsclin_start_date", "obsclin_start_time") }} obsclin_start_date
    from {{ ref('obs_clin') }}
    where obsclin_code = '11977-6'
),

renamed as (
    select
        cohort.birthid,
        avg(obs_clin.parity) as parity
    from cohort
    left join obs_clin on cohort.mother_patid = obs_clin.patid
     and obsclin_start_date between estimated_pregnancy_date and baby_birth_date
    group by cohort.birthid
)

select * from renamed