select
    a.birthid,
    a.bpg_deprs as 'BPG_DEPRS',
    b.income7 as 'INCOME7',
    c.mat_age_pu as 'MAT_AGE_PU',
    d.pay as 'PAY',
    d.insmed as 'INSMED',
    d.inswork as 'INSWORK',
    d.hi_work as 'HI_WORK',
    d.hi_medic as 'HI_MEDIC',
    e.pgwt_gn as 'PGWT_GN',
    e.mom_bmi as 'MOM_BMI',
    g.smk6c_nw as 'SMK6C_NW',
    f.pre_rx as 'PRE_RX',
    g.smk2yrs as 'SMK2YRS',
    g.smk6nw_a as 'SMK6NW_A',
    e.mom_bmig_bc as 'MOM_BMIG_BC',
    e.mat_prwt as 'MAT_PRWT',
    h.pnc_wks as 'PNC_WKS',
    (case when i.earliest_ppd_diagnosis_date is not null then 1 else 0 end) as ppd_label
from {{ ref('int_pram_bpg_deprs') }} a
left join {{ ref('int_pram_income7') }} b on a.birthid = b.birthid
left join {{ ref('int_pram_mat_age_pu') }} c on a.birthid = c.birthid
left join {{ ref('int_pram_pay') }} d on a.birthid = d.birthid
left join {{ ref('int_pram_wt_features') }} e on a.birthid = e.birthid
left join {{ ref('int_pram_pre_rx') }} f on a.birthid = f.birthid
left join {{ ref('int_pram_smoking') }} g on a.birthid = g.birthid
left join {{ ref('int_pram_pnc_weeks') }} h on a.birthid = h.birthid
left join {{ ref('int_postpartum_depression') }} i on a.birthid = i.birthid