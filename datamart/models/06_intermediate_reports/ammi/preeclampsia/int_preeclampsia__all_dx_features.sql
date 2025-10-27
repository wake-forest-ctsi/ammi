{{ 
    all_dx_features_macro(
      ref('int_preeclampsia__cohort'),
      50,
      "cohort.estimated_preg_start_date", 
      "dateadd(week, 20, cohort.estimated_preg_start_date)"
    ) 
}}