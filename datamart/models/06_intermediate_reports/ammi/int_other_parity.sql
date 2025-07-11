with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

other_parity as (
    select
        patid,
        case when dx like 'Z34.0%' then 1 else 0 end as 'parity_1_recovered',
        case when dx like 'Z34.8%' then 1 else 0 end as 'parity_2_recovered',
        dx_date
    from {{ ref('diagnosis') }}
    where dx like 'Z34.0%' or dx like 'Z34.8%'
),

renamed as (
    select
        cohort.birthid,
        coalesce(max(other_parity.parity_1_recovered), 0) as 'parity_1_recovered',
        coalesce(max(other_parity.parity_2_recovered), 0) as 'parity_2_recovered'
    from cohort
    left join other_parity on cohort.mother_patid = other_parity.patid
     and dx_date between cohort.estimated_preg_start_date and cohort.baby_birth_date
    group by cohort.birthid
)

select * from renamed