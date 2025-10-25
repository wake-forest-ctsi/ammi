{{ 
    all_dx_features_macro(
      ref('int_depression__cohort'),
      10,
      "dateadd(year, -2, cohort.baby_birth_date)", 
      "cohort.baby_birth_date"
    ) 
}}