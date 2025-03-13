with rx_grouped as (
    select
        birthid,
        rx
    from {{ ref('int_search_rx') }}
    where rx is not null -- this doesn't need to be added to the crosstab
)

select
    a.*,
    case when b.earliest_ppd_diagnosis_date is not null then 1 else 0 end as F53_diagnosis,
    case when c.phq9_total_max > 9 then 1 else 0 end as phq9_diagnosis,
    case when c.phq9_total_max is null then 1 else 0 end as phq9_isna,
    case when d.edinburgh_depression_total_max > 9 then 1 else 0 end as edinburgh_diagnosis,
    case when d.edinburgh_depression_total_max is null then 1 else 0 end as edinburgh_isna
from rx_grouped a
left join {{ ref('int_postpartum_depression') }} b on a.birthid = b.birthid
left join {{ ref('int_phq9_after_delivery') }} c on a.birthid = c.birthid
left join {{ ref('int_edinburgh_after_delivery') }} d on a.birthid = d.birthid