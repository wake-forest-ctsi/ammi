{{ config(materialized='view', ) }}

with vital_tmp as
(
select
  int_preeclampsia_study_dates.birth_id,
  vital.measure_date + vital.measure_time as 'measure_date',
  vital.systolic,
  vital.diastolic,
  case when (vital.systolic >= 160) or (vital.diastolic >= 110) then 2
	   when (vital.systolic >= 140) or (vital.diastolic >= 90) then 1	
	   else 0 end as "bp_cat",
  case when vital.measure_date < dateadd(week, 20, int_preeclampsia_study_dates.estimated_pregnancy_start_date) then 0
       when vital.measure_date between dateadd(week, 20, int_preeclampsia_study_dates.estimated_pregnancy_start_date) and dateadd(day, 90, int_preeclampsia_study_dates.delivery_date) then 1 end as "period"
from 
    {{ ref('int_preeclampsia_study_dates')}} int_preeclampsia_study_dates
    inner join {{ ref('vital') }} on vital.patid = int_preeclampsia_study_dates.baby_id
        and (vital.systolic is not null and vital.diastolic is not null and vital.measure_date is not null)
        and vital.measure_date <= dateadd(day, 90, int_preeclampsia_study_dates.delivery_date)
)
-- the 4h criteria
,vital_tmp_4h as
(
select
  vital_tmp.birth_id,
  vital_tmp.period,
  vital_tmp.bp_cat
from 
    vital_tmp
where 
    vital_tmp.bp_cat > 0
group by 
    vital_tmp.birth_id
    , vital_tmp.period
    , vital_tmp.bp_cat
having 
    datediff(hour, min(vital_tmp.measure_date), max(vital_tmp.measure_date)) >= 4 
)
-- get the max bp_cat for each patient and period
,bp_cat_tmp as
(
select
  vital_tmp_4h.birth_id,
  vital_tmp_4h.period,
  max(vital_tmp_4h.bp_cat) as 'bp_cat'
from 
    vital_tmp_4h
group by 
    vital_tmp_4h.birth_id
    , vital_tmp_4h.period
)

-- the bp cat variable
,bp_cat as
(
select
  int_preeclampsia_study_dates.birth_id,
  case when b.bp_cat_before_20wk is null then 0 else b.bp_cat_before_20wk end as 'bp_cat_before_20wk',
  case when c.bp_cat_after_20wk is null then 0 else c.bp_cat_after_20wk end as 'bp_cat_after_20wk'
from 
    {{ ref('int_preeclampsia_study_dates') }} int_preeclampsia_study_dates
    left join (select birth_id, bp_cat as 'bp_cat_before_20wk' from bp_cat_tmp where period = 0) b on int_preeclampsia_study_dates.birth_id = b.birth_id
    left join (select birth_id, bp_cat as 'bp_cat_after_20wk' from bp_cat_tmp where period = 1) c on int_preeclampsia_study_dates.birth_id = c.birth_id
    )
-- chronic hypertension
,chtn as 
(
select
  int_preeclampsia_study_dates.birth_id,
  case when max(b.bp_cat_before_20wk) > 0 then 1 
       when max(case when dx_date is not null then 1 else 0 end) > 0 then 1 
	   else 0 end as 'chtn_any'
from 
    {{ ref('int_preeclampsia_study_dates') }} int_preeclampsia_study_dates
    left join bp_cat b on int_preeclampsia_study_dates.birth_id = b.birth_id
    left join (select * from {{ ref('diagnosis') }} where dx like 'i1%') c on int_preeclampsia_study_dates.mother_id = c.patid 
        and c.dx_date < dateadd(week, 20, int_preeclampsia_study_dates.estimated_pregnancy_start_date)
group by 
    int_preeclampsia_study_dates.birth_id
)
-- magnisium infusion
,mag_infuse as
(
select
  int_preeclampsia_study_dates.birth_id,
  max(case when b.medadmin_start_date is not null then 1 else 0 end) as 'mag_infuse'
from 
    {{ ref('int_preeclampsia_study_dates') }} int_preeclampsia_study_dates
    left join {{ ref('med_admin') }} b on int_preeclampsia_study_dates.mother_id = b.patid
        and b.raw_medadmin_med_name like '%magnesium sulfate%'
        and b.medadmin_start_date between dateadd(week, 20, int_preeclampsia_study_dates.estimated_pregnancy_start_date) and dateadd(day, 90, int_preeclampsia_study_dates.delivery_date)
group by 
    int_preeclampsia_study_dates.birth_id
)

-- pre-eclampsia with severe feature or eclampsia
,icd_10_pre_sf as
(
select
  a.birth_id,
  max(case when b.dx_date is not null then 1 else 0 end) as 'pre_sf_or_ecl'
from 
    {{ ref('int_preeclampsia_study_dates') }} a
    left join {{ ref('diagnosis') }} b on a.mother_id = b.patid
        and (b.dx like 'o14.1%' or b.dx like 'o14.2%' or b.dx like 'o15%')
        and b.dx_date between dateadd(week, 20, a.estimated_pregnancy_start_date) and dateadd(day, 90, a.delivery_date)
group by 
    a.birth_id
)
-- pre-eclampsia with chronic hyptertension
,icd_10_sipe as
(
select
  a.birth_id,
  max(case when b.dx_date is not null then 1 else 0 end) as 'sipe'
from 
    {{ ref('int_preeclampsia_study_dates') }} a
    left join {{ ref('diagnosis') }} b on a.mother_id = b.patid
        and b.dx like 'o11%'
        and b.dx_date between dateadd(week, 20, a.estimated_pregnancy_start_date) and dateadd(day, 90, a.delivery_date)
group by 
    a.birth_id
)
-- proteinuria 
,lab_proteinuria as
(
select
  a.birth_id,
  max(case when b.lab_order_date is not null then 1 else 0 end) as 'lab_proteinuria'
from 
    {{ ref('int_preeclampsia_study_dates') }} a
    left join {{ ref('lab_result_cm') }} b on a.mother_id = b.patid
        and b.lab_order_date between dateadd(week, 20, a.estimated_pregnancy_start_date) and dateadd(day, 90, a.delivery_date)
        and (
            (lab_loinc = '2889-4' and -- protein in 24h urine > 300mg
            ((substring(lower(result_unit),1,2)='mg' and result_num >= 300) or 
            (substring(lower(result_unit),1,1)='g' and result_num >= 0.3))) 
        or (lab_loinc = '2890-2' and -- urine protein/creatine (upc) > 0.3
            ((substring(lower(result_unit),1,2)='mg' and result_num >= 300) or
            (substring(lower(result_unit),1,2)='mg' and norm_range_high='<=0.15' and result_num >= 0.3) or
            (substring(lower(result_unit),1,1)='g' and result_num >= 0.3)))
        )
group by 
    a.birth_id
)
-- other lab
,lab_others as
(
select
  a.birth_id,
  max(case when b.lab_order_date is not null then 1 else 0 end) as 'lab_others'
from 
    {{ ref('int_preeclampsia_study_dates') }} a
    left join {{ ref('lab_result_cm') }} b on a.mother_id = b.patid
        and b.lab_order_date between dateadd(week, 20, a.estimated_pregnancy_start_date) and dateadd(day, 90, a.delivery_date)
        and (
            (lab_loinc = '1920-8' and result_num >= 66) -- ast >= 66
        or (lab_loinc = '1742-6' and result_num >= 72) -- alt >= 72
        or (lab_loinc = '2160-0' and lower(result_unit) = 'mg/dl' and result_num > 1.1 and result_num < 10) -- ct >1.1, there're too many lab with the same loinc code; there's also unit ml/min not sure what to do
        or (lab_loinc = '777-3' and substring(result_unit,1,2) = '10' and result_num < 100) -- plt < 100
        )
group by 
    a.birth_id
)

-- finally the decision table
,decision_table as
(
select
    a.birth_id,
    a.pre_sf_or_ecl,
    b.mag_infuse,
    c.chtn_any,
    d.bp_cat_after_20wk,
    e.sipe,
    f.lab_proteinuria,
    g.lab_others
from 
    icd_10_pre_sf a
    left join mag_infuse b on a.birth_id = b.birth_id
    left join chtn c on a.birth_id = c.birth_id
    left join bp_cat d on a.birth_id = d.birth_id
    left join icd_10_sipe e on a.birth_id = e.birth_id
    left join lab_proteinuria f on a.birth_id = f.birth_id
    left join lab_others g on a.birth_id = g.birth_id
)

select 
  *,
  case when (chtn_any = 0 and (pre_sf_or_ecl = 1 or bp_cat_after_20wk = 2 or mag_infuse = 1 or sipe = 1)) then 1  -- i think sipe can be 1 here
       when (chtn_any = 0 and bp_cat_after_20wk = 1 and lab_others = 1) then 1 -- seems protienuria alone is not considered severe??
	   when (chtn_any = 1 and (pre_sf_or_ecl = 1 or sipe = 1 or mag_infuse = 1)) then 1
	   when (chtn_any = 1 and bp_cat_after_20wk = 2 and lab_proteinuria = 1) then 1 -- this doesn't feel right to me, should include lab_others = 1??
	   when (chtn_any = 1 and bp_cat_after_20wk = 1 and lab_others = 1) then 1
	   else 0 end as 'preeclampsia_label'
from 
    decision_table