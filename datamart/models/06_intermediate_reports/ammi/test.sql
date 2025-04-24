{{ 
    dx_features_macro(
      "int_depression__selected_dx",
      "dateadd(year, -2, cohort.baby_birth_date)", 
      "cohort.baby_birth_date"
    ) 
}}