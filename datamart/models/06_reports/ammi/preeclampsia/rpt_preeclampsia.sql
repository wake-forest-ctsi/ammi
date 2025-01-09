-- final data mart for preeclampsia, tried to reproduce kibria's table

select
    a.birthid,
    a.baby_birth_date,
    a.mother_patid,
    b.preeclampsia as preeclampsia_label,
    c.wt_mean,
    c.wt_max,
    c.wt_min,
    c.original_bmi_mean,
    c.original_bmi_max,
    c.original_bmi_min,
    c.computed_bmi_mean,
    c.computed_bmi_max,
    c.computed_bmi_min,
    {{ dbt_utils.star(from=ref('int_bp_features_lifetime'), except=['birthid'], relation_alias='i')}},
    {{ dbt_utils.star(from=ref('int_obs_clin_features'), except=['birthid']) }},
    e.mother_age,
    f.mother_height,
    {{ dbt_utils.star(from=ref('int_preeclampsia_rx'), except=['birthid']) }},
    h.mother_is_hispanic,
    h.mother_is_black,
    h.mother_is_white
from {{ ref('int_cohort') }} a
left join {{ ref('int_preeclampsia_severe') }} b on a.birthid = b.birthid
left join {{ ref('int_vital_features') }} c on a.birthid = c.birthid
left join {{ ref('int_obs_clin_features') }} d on a.birthid = d.birthid
left join {{ ref('int_mother_age_at_birth') }} e on a.birthid = e.birthid
left join {{ ref('int_mother_height') }} f on a.birthid = f.birthid
left join {{ ref('int_preeclampsia_rx') }} g on a.birthid = g.birthid
left join {{ ref('int_race')}} h on a.mother_patid = h.mother_patid
left join {{ ref('int_bp_features_lifetime') }} i on a.birthid = i.birthid