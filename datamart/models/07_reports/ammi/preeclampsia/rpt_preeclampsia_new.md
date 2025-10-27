{% docs rpt_preeclampsia_new %}

### Summary

This table contains all of the features needed for building a predictive model for preeclampsia. All predictors (features) are computed between the estimated pregnancy start time and 20 weeks into pregnancy. 

### Feature Categories

| Category | Source Model | Notes | Example Features |
| ----------|-------------|--------|-----------------|
| baseline | multiple | Baseline features that are independent of study periods | delivery_year, mother_age_at_birth, mother_height etc |
| phenotype | multiple | Phenotype of preeclampsia | target_variable |
| diagnosis | diagnosis | Indicate whether patients has a certain diagnosis, if so, the earliest and latest days since estimated_preg_start_date. For diagnosis code, we truncated them down to 5 chararacters, except for O, T, Z codes where we keep all characters are used. We only used codes that are recorde by >50 births. | R10_9_has_dx, O09_521_ealiest_day |
| prescribing | prescribing | Indicate whether patients has a certain medication prescribed to them, if so, the earliest and latest days of the prescription since estimated_preg_start_date. We use rxnorm_cui as code identification. We only used codes that are recorde by >50 births.| 1375954_earliest_day, 90176_has_rx |
| encounter_counts | encounter | Counts of patients visit for different encounter types, including the total counts | enc_type_AV_count, total_visits_count |
| insurance | encounter | Indicates the insurance types a patients was used | insurance_1, insurance_51|
| vital | vital | Aggregated values of vitals | systolic_max_value, computed_bmi_min_value |
---

{% enddocs %}