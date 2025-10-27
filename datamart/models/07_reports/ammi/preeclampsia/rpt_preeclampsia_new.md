{% docs rpt_preeclampsia_new %}

### Summary

This table contains all of the features needed for building a predictive model for preeclampsia. All predictors (features) are computed between the estimated pregnancy start time and 20 weeks into pregnancy. 

### Feature Categories

| Category | Source Model | Notes | Example Features |
|----------|--------------|-------|-----------------|
| baseline | multiple | Baseline features independent of study periods | delivery_year, mother_age_at_birth, mother_height, etc. |
| phenotype | multiple | Phenotype of preeclampsia | target_variable |
| diagnosis | diagnosis | Indicates whether a patient has a certain diagnosis. If present, the earliest and latest days since estimated_preg_start_date are recorded. Diagnosis codes are truncated to 5 characters, except for O, T, Z codes where all characters are retained. Only codes recorded in >50 births are used. | R10_9_has_dx, O09_521_earliest_day |
| prescribing | prescribing | Indicates whether a patient has been prescribed a certain medication. If present, the earliest and latest days since estimated_preg_start_dat are recorded. Rxnorm_CUI is used for identification. Only codes recorded in >50 births are used. | 1375954_earliest_day, 90176_has_rx |
| encounter_counts | encounter | Counts of patient visits by encounter type, including total visits | enc_type_AV_count, total_visits_count |
| insurance | encounter | Indicates the insurance types a patient used | insurance_1, insurance_51 |
| vital | vital | Aggregated values of vital signs | systolic_max_value, computed_bmi_min_value |
| obs_clin | to do | | |
---

{% enddocs %}