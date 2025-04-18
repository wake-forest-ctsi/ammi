with cohort as (
    select
        mother_patid
    from {{ ref('int_cohort') }}
    group by mother_patid
),

demographic as (
    select
        patid,
        hispanic,
        race
    from {{ ref('demographic') }}
),

renamed as (
    select
        a.mother_patid,
        (case when b.hispanic = 'Y' then 1 else 0 end) as mother_is_hispanic,
        (case when b.race = '05' then 1 else 0 end) as mother_is_white,
        (case when b.race = '03' then 1 else 0 end) as mother_is_black
    from cohort a
    left join demographic b on a.mother_patid = b.patid
)

select * from renamed