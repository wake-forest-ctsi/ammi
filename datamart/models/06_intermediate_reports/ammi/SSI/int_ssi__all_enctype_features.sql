{{
    all_enctype_features_macro(
        ref('int_ssi__cohort'),
        "dateadd(day, -268, cohort.baby_birth_date)",
        "dateadd(day, -1, cohort.baby_birth_date)"
    )
}}