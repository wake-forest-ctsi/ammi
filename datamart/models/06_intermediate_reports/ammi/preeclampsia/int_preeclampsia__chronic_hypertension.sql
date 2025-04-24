{{ 
    hyptertension_macro(
      "int_preeclampsia__chronic_bp_cat",
      "cast('19000101' as date)", 
      "dateadd(week, 20, cohort.estimated_pregnancy_date)"
    ) 
}}