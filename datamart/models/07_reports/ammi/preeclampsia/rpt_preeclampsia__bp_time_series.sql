select
    a.birthid,
    b.preg_weeks,
    b.systolic,
    b.diastolic,
    c.chronic_hyptertension as chtn_any,
    d.preeclampsia as cp_anypree_sf
from {{ ref('int_cohort') }} a
left join {{ ref('int_preeclampsia__bp_time_series') }} b on a.birthid = b.birthid
left join {{ ref('int_preeclampsia__chronic_hypertension') }} c on a.birthid = c.birthid
left join {{ ref('int_preeclampsia__severe') }} d on a.birthid = d.birthid
where b.systolic is not null and b.diastolic is not null
