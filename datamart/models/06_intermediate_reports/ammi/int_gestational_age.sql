-- gestational age is filled to 268 if no records is found
-- this is called by the int_cohort which requires an estimate of preganancy start date

with obs_clin as (
    select 
        encounterid,
        min(obsclin_result_num) as gest_age_in_days
    from {{ ref('obs_clin') }}
    where obsclin_type = 'SM' and obsclin_code = '444135009'
    group by encounterid  -- potential error for twins since it's group by encountered
),

birth_relationship as (
    select
        birthid,
        mother_encounterid
    from {{ ref('birth_relationship') }}   
),

renamed as (
    select
        a.birthid,
        coalesce(b.gest_age_in_days, 268) as gest_age_in_days,
        (case when b.gest_age_in_days is null then 1 else 0 end) as gest_age_is_null
    from birth_relationship a
    left join obs_clin b on a.mother_encounterid = b.encounterid
)

select * from renamed

