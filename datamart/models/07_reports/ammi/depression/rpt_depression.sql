{% set phq_list = ('21012948', '21012949', '21012950', '21012951', '21012953',
                   '21012954', '21012955', '21012956', '21012958',
                   '99046', '71354', '21012959') %}

select
    a.birthid,
    datepart(year, a.baby_birth_date) as delivery_year,
    b.mother_age,
    c.mother_is_black,
    c.mother_is_hispanic,
    c.mother_is_white,
    d.gest_age_in_days,
    d.gest_age_is_null,
    {{ dbt_utils.star(from=ref('int_depression__insurance'), except=['birthid']) }},
    {{ dbt_utils.star(from=ref('int_depression__visit_pattern'), except=['birthid']) }},
    {{ dbt_utils.star(from=ref('int_depression__dx_features'), except=['birthid']) }},
    {{ dbt_utils.star(from=ref('int_depression__rx_features'), except=['birthid']) }},
    {{ dbt_utils.star(from=ref('int_delivery_mode_pivoted'), except=['birthid']) }},
    j.parity,
    k.parity_1_recovered,
    k.parity_2_recovered,
    coalesce(l.original_bmi_mean, l.computed_bmi_mean) as bmi,
    m.smoking,
    m.tobacco,
    n.morbid,
    n.obese,
    n.overweight,
    n.O99_obese,
    {{ dbt_utils.star(from=ref('int_censustract_features'), 
                      except=['birthid', 'addressid', 'zipcode', 'tractfips', 'longitude', 'latitude']) }},
    p.mother_height,
    {% for col in phq_list %}
        coalesce(s.phq_or_edinburgh_{{col}}_max, 0) as 'phq_or_edinburgh_{{col}}_max',
        case when s.phq_or_edinburgh_{{col}}_max is null then 1 else 0 end as 'phq_or_edinburgh_{{col}}_isna',
    {% endfor %}
    (case when q.earliest_ppd_diagnosis_date is not null then 1 else 0 end) as F53_label,
    (case when q.earliest_ppd_diagnosis_date_delete is not null then 1 else 0 end) as PPD_delete_label,
    r.edinburgh_max,
    t.phq9_total_max
from {{ ref('int_cohort') }} a
left join {{ ref('int_mother_age_at_birth') }} b on a.birthid = b.birthid
left join {{ ref('int_race') }} c on a.mother_patid = c.mother_patid
left join {{ ref('int_gestational_age') }} d on a.birthid = d.birthid
left join {{ ref('int_depression__insurance') }} e on a.birthid = e.birthid
left join {{ ref('int_depression__visit_pattern') }} f on a.birthid = f.birthid
left join {{ ref('int_depression__dx_features') }} g on a.birthid = g.birthid
left join {{ ref('int_depression__rx_features') }} h on a.birthid = h.birthid
left join {{ ref('int_delivery_mode_pivoted') }} i on a.birthid = i.birthid
left join {{ ref('int_parity') }} j on a.birthid = j.birthid
left join {{ ref('int_other_parity') }} k on a.birthid = k.birthid
left join {{ ref('int_depression__vital_features') }} l on a.birthid = l.birthid
left join {{ ref('int_depression__smoking') }} m on a.birthid = m.birthid
left join {{ ref('int_depression__other_obese') }} n on a.birthid = n.birthid
left join {{ ref('int_censustract_features') }} o on a.birthid = o.birthid
left join {{ ref('int_mother_height') }} p on a.birthid = p.birthid
left join {{ ref('int_depression__postpartum_depression_dx_code_only') }} q on a.birthid = q.birthid
left join {{ ref('int_depression__edinburgh_after_delivery') }} r on a.birthid = r.birthid
left join {{ ref('int_depression__phq_or_edinburgh') }} s on a.birthid = s.birthid
left join {{ ref('int_depression__phq9_after_delivery') }} t on a.birthid = t.birthid
where a.baby_birth_date < '2023-05-31'