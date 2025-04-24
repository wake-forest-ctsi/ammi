-- this is to reproduce unc's definition

{% set date_range_list = ("dateadd(week, 20, cohort.estimated_pregnancy_date)",
                          "dateadd(day, 90, cohort.baby_birth_date)") %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

mag_infuse as (
    select
        cohort.birthid,
        max(case when medadmin_start_date is not null then 1 else 0 end) as mag_infuse
    from cohort
    left join {{ ref('med_admin') }} a on a.patid = cohort.mother_patid
     and {{ add_time_to_date_macro("a.medadmin_start_date", "a.medadmin_start_time") }} between {{ date_range_list[0] }} and {{ date_range_list[1] }}
     and raw_medadmin_med_name like '%MAGNESIUM SULFATE%'
    group by cohort.birthid
),

-- pre-eclampsia with severe feature or eclampsia
icd_10_pre_sf as (
    select
        cohort.birthid,
        max(case when dx_date is not null then 1 else 0 end) as pre_sf_or_ecl
    from cohort
    left join {{ ref('diagnosis') }} a on a.patid = cohort.mother_patid
     and a.dx_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
     and (a.dx like 'O14.1%' or a.dx like 'O14.2%' or a.dx like 'O15%')
    group by cohort.birthid
),

-- pre-eclampsia with chronic hyptertension
icd_10_sipe as (
    select
        cohort.birthid,
        max(case when dx_date is not null then 1 else 0 end) as sipe
    from cohort
    left join {{ ref('diagnosis') }} a on a.patid = cohort.mother_patid
     and a.dx_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
     and a.dx like 'O11%'
    group by cohort.birthid
),

-- Proteinuria
lab_proteinuria as (
    select
        cohort.birthid,
        max(case when specimen_date is not null then 1 else 0 end) as lab_proteinuria
    from cohort
    left join {{ ref('lab_result_cm') }} a on a.patid = cohort.mother_patid
     and {{ add_time_to_date_macro("a.specimen_date", "a.specimen_time") }}  between {{ date_range_list[0] }} and {{ date_range_list[1] }}
     and (
             (lab_loinc = '2889-4' and -- protein in 24h urine > 300mg
             ((substring(lower(result_unit),1,2)='mg' and result_num >= 300) or 
              (substring(lower(result_unit),1,1)='g' and result_num >= 0.3))) 
          or (lab_loinc = '2890-2' and -- urine protein/creatine (upc) > 0.3
             ((substring(lower(result_unit),1,2)='mg' and result_num >= 300) or
              (substring(lower(result_unit),1,2)='mg' and norm_range_high='<=0.15' and result_num >= 0.3) or
              (substring(lower(result_unit),1,1)='g' and result_num >= 0.3)))
     )
    group by cohort.birthid
),

-- other labs
lab_others as (
    select
        cohort.birthid,
        max(case when specimen_date is not null then 1 else 0 end) as lab_others
    from cohort
    left join {{ ref('lab_result_cm') }} a on a.patid = cohort.mother_patid
     and {{ add_time_to_date_macro("a.specimen_date", "a.specimen_time") }} between {{ date_range_list[0] }} and {{ date_range_list[1] }}
     and (
           (lab_loinc = '1920-8' and result_num >= 66) -- ast >= 66
        or (lab_loinc = '1742-6' and result_num >= 72) -- alt >= 72
        or (lab_loinc = '2160-0' and lower(result_unit) = 'mg/dl' and result_num > 1.1 and result_num < 10) -- ct >1.1, there're too many lab with the same loinc code; there's also unit ml/min not sure what to do
        or (lab_loinc = '777-3' and substring(result_unit,1,2) = '10' and result_num < 100) -- plt < 100
     )
    group by cohort.birthid
),

decision_table as (
    select
        a.birthid,
        a.pre_sf_or_ecl,
        b.mag_infuse,
        c.chronic_hyptertension as chtn_any,
        d.bp_cat as bp_cat_after_20wk,
        e.sipe,
        f.lab_proteinuria,
        g.lab_others
    from icd_10_pre_sf a
    left join mag_infuse b on a.birthid = b.birthid
    left join {{ ref('int_preeclampsia__chronic_hypertension') }} c on a.birthid = c.birthid
    left join {{ ref('int_preeclampsia__bp_cat') }} d on a.birthid = d.birthid
    left join icd_10_sipe e on a.birthid = e.birthid
    left join lab_proteinuria f on a.birthid = f.birthid
    left join lab_others g on a.birthid = g.birthid
),

renamed as (
    select
        birthid,
        case when (chtn_any = 0 and (pre_sf_or_ecl = 1 or bp_cat_after_20wk = 2 or mag_infuse = 1 or sipe = 1)) then 1  -- i think sipe can be 1 here
             when (chtn_any = 0 and (bp_cat_after_20wk = 1 and lab_others = 1)) then 1 -- seems protienuria alone is not considered severe??
	         when (chtn_any = 1 and (pre_sf_or_ecl = 1 or sipe = 1 or mag_infuse = 1)) then 1
	         when (chtn_any = 1 and bp_cat_after_20wk = 2 and lab_proteinuria = 1) then 1 -- this doesn't feel right to me, should include lab_others = 1??
	         when (chtn_any = 1 and bp_cat_after_20wk = 1 and lab_others = 1) then 1
	         else 0 end as 'preeclampsia'
    from decision_table
)

select * from renamed

