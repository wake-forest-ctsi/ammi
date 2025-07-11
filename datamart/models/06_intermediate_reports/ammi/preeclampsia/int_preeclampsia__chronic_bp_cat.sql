{{ 
    bp_cat_macro(
      "cohort.estimated_preg_start_date", 
      "dateadd(week, 20, cohort.estimated_preg_start_date)"
    ) 
}}