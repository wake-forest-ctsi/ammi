{{ 
    bp_cat_macro(
      ref('int_preeclampsia__cohort'),
      "dateadd(week, 20, cohort.estimated_preg_start_date)", 
      "dateadd(day, 90, cohort.baby_birth_date)"
    ) 
}}