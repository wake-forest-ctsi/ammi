{{
    all_insurance_features_macro(
        ref('int_ssi__cohort'),
        "dateadd(year, -1, cohort.baby_birth_date)",
        "cohort.baby_birth_date"
    )
}}