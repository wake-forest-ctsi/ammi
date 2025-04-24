{{ 
    bp_cat_macro(
      "cast('19000101' as date)", 
      "dateadd(week, 20, cohort.estimated_pregnancy_date)"
    ) 
}}