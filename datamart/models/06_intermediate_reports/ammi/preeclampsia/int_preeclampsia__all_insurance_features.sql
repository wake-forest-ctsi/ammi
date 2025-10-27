{{
    all_insurance_features_macro(
        ref('int_preeclampsia__cohort'),
        "cohort.estimated_preg_start_date", 
        "dateadd(week, 20, cohort.estimated_preg_start_date)"
    )
}}