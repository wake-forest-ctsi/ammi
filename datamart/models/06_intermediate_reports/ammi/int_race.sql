with cohort as (
    select
        birthid,
        mother_patid
    from {{ ref('int_cohort') }}
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
        a.birthid,
        a.mother_patid,
        (case when b.hispanic = 'Y' then 1 else 0 end) as mother_is_hispanic,
        (case when b.race = '05' then 1 else 0 end) as mother_is_white,
        (case when b.race = '03' then 1 else 0 end) as mother_is_black
    from cohort a
    inner join demographic b on a.mother_patid = b.patid
)

select * from renamed