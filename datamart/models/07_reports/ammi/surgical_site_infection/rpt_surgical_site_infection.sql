select
    cohort.birthid,
    cohort.mother_patid,
    csection.delivery_mode,
    enc_type.enc_type,
    enc_type.admitting_source,
    ssi_diagnosis.SSI_diagnosis
from {{ ref('int_cohort') }} cohort
inner join {{ ref('int_c_section') }} csection on cohort.birthid = csection.birthid
left join {{ ref('int_delivery_enc_type') }} enc_type on cohort.birthid = enc_type.birthid
left join {{ ref('int_ssi__diagnosis') }} ssi_diagnosis on cohort.birthid = ssi_diagnosis.birthid