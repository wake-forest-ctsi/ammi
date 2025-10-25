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

parity_tmp as (
    select
        cohort.birthid,
        obs_clin.parity as parity,
        obsclin_start_date,
        baby_birth_date,
        row_number() over (partition by cohort.birthid order by obsclin_start_date desc) as k
    from cohort
    inner join obs_clin on cohort.mother_patid = obs_clin.patid
     and obsclin_start_date between estimated_preg_start_date and dateadd(day, 30, baby_birth_date)
),

renamed as (
    select
      birthid,
      case 
        when (obsclin_start_date > baby_birth_date) then parity - 1
        else parity 
      end as parity
    from parity_tmp
    where k = 1
)

select * from renamed