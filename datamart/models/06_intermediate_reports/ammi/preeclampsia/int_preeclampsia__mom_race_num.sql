with cohort as (
    select
        birthid,
        mother_patid
    from {{ ref('int_cohort') }}
),

race as (
    select
        patid,
        race,
        raw_race
    from {{ ref('demographic') }}
),

renamed as (
    select
        a.birthid,
        case
            when race = '01' then 1             -- American Indian or Alaska
            when race = '02' then 2             -- Asian
            when race = '03' then 3             -- Black or African American
            when race = 'OT' and raw_race = 'H' then 4 -- Multiracial Native Hawaiian/Other
            when race = '04' and raw_race = 'P' then 5 -- Pacific Islander
            when race = 'NI' or race = 'UN' or race = '07' then 6 -- unknown
            when race = '05' then 8             -- white
            else 7                              -- Other race
        end as mom_race_num,
        race,
        raw_race
    from cohort a
    left join race b on a.mother_patid= b.patid
)

select * from renamed