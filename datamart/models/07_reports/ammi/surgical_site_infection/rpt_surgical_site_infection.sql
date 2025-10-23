select
    cohort.birthid,
    cohort.mother_patid,
    phenotype.SSI_diagnosis,
    phenotype.wound_culture,
    datepart(year, cohort.baby_birth_date) as delivery_year,
    mother_age.mother_age,
    mother_height.mother_height,
    race.mother_is_black,
    race.mother_is_hispanic,
    race.mother_is_white,
    gestage.gest_age_in_days,
    gestage.gest_age_is_null,
    parity.parity,
    other_parity.parity_1_recovered,
    other_parity.parity_2_recovered,
    {{ dbt_utils.star(from=ref('int_delivery_mode_pivoted'), except=['birthid']) }},
    {{ dbt_utils.star(from=ref('int_delivery_enc_type_pivoted'), except=['birthid']) }},
    {{ dbt_utils.star(from=ref('int_ssi__visit_pattern'), except=['birthid']) }}
    {{ dbt_utils.star(from=ref('int_ssi__insurance'), except=['birthid']) }}
from {{ ref('int_ssi__cohort') }} cohort
left join {{ ref('int_ssi__phenotype') }} phenotype on cohort.birthid = phenotype.birthid
left join {{ ref('int_mother_age_at_birth') }} mother_age on cohort.birthid = mother_age.birthid
left join {{ ref('int_mother_height') }} mother_height on cohort.birthid = mother_height.birthid
left join {{ ref('int_race') }} race on cohort.mother_patid = race.mother_patid
left join {{ ref('int_gestational_age') }} gestage on cohort.birthid = gestage.birthid
left join {{ ref('int_parity') }} parity on cohort.birthid = parity.birthid
left join {{ ref('int_other_parity') }} other_parity on cohort.birthid = other_parity.birthid
left join {{ ref('int_delivery_mode_pivoted') }} delivery_mode on cohort.birthid = delivery_mode.birthid
left join {{ ref('int_delivery_enc_type_pivoted') }} enc_type on cohort.birthid = enc_type.birthid
left join {{ ref('int_ssi__visit_pattern') }} visit on cohort.birthid = visit.birthid
left join {{ ref('int_ssi__insurance') }} insurance on cohort.birthid = insurance.birthid