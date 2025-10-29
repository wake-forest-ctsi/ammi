{{ 
    all_lab_text_features_macro(
      ref('int_ssi__cohort'),
      10,
      "dateadd(year, -1, cohort.baby_birth_date)", 
      "cohort.baby_birth_date"
    ) 
}}