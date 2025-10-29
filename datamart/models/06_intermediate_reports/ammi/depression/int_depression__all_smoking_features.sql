{{
    all_smoking_macro(
        ref('int_depression__cohort'),
        "dateadd(year, -2, cohort.baby_birth_date)",
        "cohort.baby_birth_date"
    )
}}