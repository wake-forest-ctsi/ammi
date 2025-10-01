{{ 
    hyptertension_macro(
      "int_preeclampsia__chronic_bp_cat",
      "cohort.estimated_preg_start_date", 
      "dateadd(week, 20, cohort.estimated_preg_start_date)"
    ) 
}}
