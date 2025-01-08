with visits as (
    select distinct  -- distinct here is necessary to remove duplicated entries 
        birthid,
        days_since_child_birth
    from {{ ref('int_visits') }}
),

renamed as (
    select
        birthid,
        sum(case when days_since_child_birth between -268 and 1 then 1 else 0 end) as counts_of_visits_prenatal_care,
        sum(case when days_since_child_birth between 1 and 90 then 1 else 0 end) as counts_of_visits_3m_after_delivery,
        sum(case when days_since_child_birth between 1 and 180 then 1 else 0 end) as counts_of_visits_6m_after_delivery
    from visits
    group by birthid
)

select * from renamed