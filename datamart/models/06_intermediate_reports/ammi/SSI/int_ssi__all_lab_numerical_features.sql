{{ 
    all_lab_numerical_features_macro(
      10,
      "dateadd(year, -1, cohort.baby_birth_date)", 
      "cohort.baby_birth_date",
      "inner join " ~ ref('int_c_section') ~ "csection on cohort.birthid = csection.birthid"
    ) 
}}