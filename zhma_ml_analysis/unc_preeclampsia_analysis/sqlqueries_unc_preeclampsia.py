demographic_base_sql_string = '''
WITH gestage_tmp AS
(
SELECT 
  ENCOUNTERID,
  MIN(OBSCLIN_RESULT_NUM) AS "gest_age_in_days"
FROM AMMI.dbo.OBS_CLIN
WHERE OBSCLIN_TYPE = 'SM' AND OBSCLIN_CODE = '444135009' -- this is the code the gestational age
GROUP BY ENCOUNTERID
)
,
-- get the delivery date from birthdate of baby
delivery_date AS
(
SELECT
  a.BIRTHID,
  b.BIRTH_DATE AS 'delivery_date'  -- this already has time in it
FROM AMMI.dbo.BIRTH_RELATIONSHIP a
LEFT JOIN AMMI.dbo.DEMOGRAPHIC b ON a.PATID = b.PATID
WHERE b.BIRTH_DATE IS NOT NULL  -- <10 records are removed 
)
,
demographic_base AS
(
SELECT
  a.BIRTHID,
  a.PATID,
  a.ENCOUNTERID,
  a.PREGNANCYID,
  a.MOTHERID,
  a.MOTHER_ENCOUNTERID,
  c.gest_age_in_days,
  DATEADD(DAY, -c.gest_age_in_days, b.delivery_date) AS "preg_start_date",
  b.delivery_date AS "preg_end_date",
  d.ADMIT_DATE AS "delivery_admission_date", -- this already has time in it
  d.DISCHARGE_DATE AS "delivery_discharge_date",
  DATEADD(DAY, -c.gest_age_in_days + 140, b.delivery_date) AS "cutoff_time" -- default cut off time is 20 week
FROM AMMI.dbo.BIRTH_RELATIONSHIP a
INNER JOIN delivery_date b ON a.BIRTHID = b.BIRTHID
INNER JOIN gestage_tmp c ON a.MOTHER_ENCOUNTERID = c.ENCOUNTERID
INNER JOIN AMMI.dbo.ENCOUNTER d ON a.MOTHER_ENCOUNTERID = d.ENCOUNTERID
)
'''

race_sql_string = demographic_base_sql_string + '''
SELECT
  a.BIRTHID,
  MAX(CASE WHEN b.HISPANIC = 'Y' THEN 1 ELSE 0 END) AS "is_hispanic",
  MAX(CASE WHEN b.RACE = '03' THEN 1 ELSE 0 END) AS "is_black"
FROM demographic_base a LEFT JOIN AMMI.dbo.DEMOGRAPHIC b ON a.MOTHERID = b.PATID
GROUP BY a.BIRTHID
ORDER BY a.BIRTHID
'''

age_sql_string = demographic_base_sql_string + '''
SELECT
  a.BIRTHID,
  a.gest_age_in_days,
  DATEDIFF(YEAR, b.BIRTH_DATE, a.preg_start_date) AS "mom_age"
FROM demographic_base a LEFT JOIN AMMI.dbo.DEMOGRAPHIC b ON a.MOTHERID = b.PATID
ORDER BY a.BIRTHID
'''

blood_pressure_sql_string_old = demographic_base_sql_string + ',' + '''
vital_tmp AS
(
SELECT
  PATID,
  CASE WHEN (SYSTOLIC < 140) AND (DIASTOLIC < 90) THEN 0 
       WHEN (SYSTOLIC BETWEEN 140 AND 159) OR (DIASTOLIC BETWEEN 90 AND 109) THEN 1
       WHEN (SYSTOLIC > 159) OR (DIASTOLIC > 109) THEN 2 END AS "bp_cat",
  MEASURE_DATE + MEASURE_TIME AS "measure_date"
FROM AMMI.dbo.VITAL
WHERE SYSTOLIC IS NOT NULL AND DIASTOLIC IS NOT NULL
)
,
-- select the time period: <20wks, 20-delivery, during delivery, 90d after delivery
vital_tmp2 AS
(
SELECT
  a.BIRTHID,
  b.bp_cat,
  b.measure_date,
  CASE WHEN b.measure_date < DATEADD(WEEK, 20, a.preg_start_date) THEN 0
       WHEN b.measure_date BETWEEN DATEADD(WEEK, 20, a.preg_start_date) AND a.delivery_admission_date THEN 1
	   WHEN b.measure_date BETWEEN a.delivery_admission_date AND a.delivery_discharge_date THEN 2
	   WHEN b.measure_date BETWEEN a.delivery_discharge_date AND DATEADD(DAY, 90, a.delivery_discharge_date) THEN 3
       ELSE NULL END AS "time_period"
FROM demographic_base a INNER JOIN vital_tmp b ON a.MOTHERID = b.PATID
)
,
-- the 4h criteria, this includes bp_cat=0, which may not be correct?
vital_tmp3 AS
(
SELECT
  BIRTHID,
  time_period,
  bp_cat
FROM vital_tmp2
WHERE time_period IS NOT NULL
GROUP BY BIRTHID, bp_cat, time_period
HAVING DATEDIFF(HOUR, MIN(measure_date), MAX(measure_date)) >= 4
)

SELECT
  BIRTHID,
  time_period,
  MAX(bp_cat) AS "bp_cat"
FROM vital_tmp3
GROUP BY BIRTHID, time_period
ORDER BY 1,2,3;
'''

blood_pressure_sql_string = demographic_base_sql_string + '''
SELECT
  a.BIRTHID,
  AVG(b.SYSTOLIC) AS 'mean_sbp_value',
  AVG(b.DIASTOLIC) AS 'mean_dbp_value',
  MAX(b.SYSTOLIC) AS 'max_sbp_value',
  MAX(b.DIASTOLIC) AS 'max_dbp_value',
  MIN(b.SYSTOLIC) AS 'min_sbp_vale',
  MIN(b.DIASTOLIC) AS 'min_dbp_value',
  AVG(b.SYSTOLIC - b.DIASTOLIC) AS 'mean_pulse_pressure',
  MAX(b.SYSTOLIC - b.DIASTOLIC) AS 'max_pulse_pressure',
  MIN(b.SYSTOLIC - b.DIASTOLIC) AS 'min_pulse_pressure'
FROM demographic_base a
LEFT JOIN AMMI.dbo.VITAL b ON a.MOTHERID = b.PATID
 AND b.measure_date < DATEADD(WEEK, 20, a.preg_start_date)
 AND b.SYSTOLIC IS NOT NULL AND b.DIASTOLIC IS NOT NULL
GROUP BY a.BIRTHID
ORDER BY a.BIRTHID
'''

vital_sql_string = demographic_base_sql_string + ',' + '''
vital_1 AS
(
SELECT DISTINCT
  a.BIRTHID,
  -- weight
  MAX(b.WT) OVER (PARTITION BY a.BIRTHID) AS 'max_WEIGHT',
  MIN(b.WT) OVER (PARTITION BY a.BIRTHID) AS 'min_WEIGHT',
  AVG(b.WT) OVER (PARTITION BY a.BIRTHID) AS 'mean_WEIGHT',
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.WT) OVER (PARTITION BY a.BIRTHID) AS 'median_WEIGHT',
  -- original bmi
  MAX(b.ORIGINAL_BMI) OVER (PARTITION BY a.BIRTHID) AS 'max_BMI',
  MIN(b.ORIGINAL_BMI) OVER (PARTITION BY a.BIRTHID) AS 'min_BMI',
  AVG(b.ORIGINAL_BMI) OVER (PARTITION BY a.BIRTHID) AS 'mean_BMI',
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.ORIGINAL_BMI) OVER (PARTITION BY a.BIRTHID) AS 'median_BMI'
FROM demographic_base a
LEFT JOIN AMMI.dbo.VITAL b ON a.MOTHERID = b.PATID AND b.MEASURE_DATE BETWEEN a.preg_start_date AND a.cutoff_time
)
,
vital_2 AS
(
SELECT DISTINCT
  a.BIRTHID,
  -- height is not likely to change so the cut off time can go further, i think use only 1 height should be enough
  -- MAX(b.HT) OVER (PARTITION BY a.BIRTHID) AS 'max_HEIGHT',
  -- MIN(b.HT) OVER (PARTITION BY a.BIRTHID) AS 'min_HEIGHT',
  AVG(b.HT) OVER (PARTITION BY a.BIRTHID) AS 'mean_HEIGHT'
  -- PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY b.HT) OVER (PARTITION BY a.BIRTHID) AS 'median_HEIGHT'
FROM demographic_base a
LEFT JOIN AMMI.dbo.VITAL b ON a.MOTHERID = b.PATID AND b.MEASURE_DATE BETWEEN a.preg_start_date AND a.preg_end_date
)

SELECT
  a.*,
  -- b.max_HEIGHT,
  -- b.min_HEIGHT,
  b.mean_HEIGHT
  -- b.median_HEIGHT
FROM vital_1 a LEFT JOIN vital_2 b ON a.BIRTHID = b.BIRTHID
ORDER BY a.BIRTHID
'''

obs_sql_string = demographic_base_sql_string + ',' + '''
obs_tmp AS
(
SELECT
  a.BIRTHID,
  b.RAW_OBSCLIN_NAME,
  b.OBSCLIN_RESULT_NUM
FROM demographic_base a
LEFT JOIN AMMI.dbo.OBS_CLIN b ON a.MOTHERID = b.PATID AND b.OBSCLIN_START_DATE BETWEEN a.preg_start_date AND a.cutoff_time
WHERE RAW_OBSCLIN_NAME = 'Pulse Heart Rate' OR RAW_OBSCLIN_NAME = 'MAP Mean Blood Pressure'
   OR RAW_OBSCLIN_NAME = 'RespiratoryRate' OR RAW_OBSCLIN_NAME = 'SPO2 Oxygen Saturation'
   OR RAW_OBSCLIN_NAME = 'Body Temperature'
)

SELECT DISTINCT
  BIRTHID,
  RAW_OBSCLIN_NAME,
  MIN(OBSCLIN_RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_OBSCLIN_NAME) AS 'min_VALUE',
  MAX(OBSCLIN_RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_OBSCLIN_NAME) AS 'max_VALUE',
  AVG(OBSCLIN_RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_OBSCLIN_NAME) AS 'mean_VALUE',
  PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY OBSCLIN_RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_OBSCLIN_NAME) AS 'median_VALUE'
FROM obs_tmp
ORDER BY BIRTHID;
'''

preeclampsia_sql_string_old = demographic_base_sql_string + ',' + '''
preeclampsia_diagnosis AS
(
select 
    a.BIRTHID,
    min(a.preg_start_date) as 'preg_start_date',
    min(b.dx_date) as 'earliest_diagnosis_date',
    max(b.dx_date) as 'latest_diagnosis_date'
from 
    demographic_base a
    left join 
    (select dx_type, dx, dx_date, patid 
    from AMMI.dbo.DIAGNOSIS
    where dx_type = '10' and (dx like 'O14%' or dx like 'O15%')) b
    on a.motherid = b.patid
        and b.dx_date between a.preg_start_date AND a.preg_end_date
group by a.BIRTHID
)
,
/* Birth episodes with preeclampsia supporting laboratory measurements */
lab_tmp as
(
select 
    *
from AMMI.dbo.LAB_RESULT_CM a
where 
   (a.lab_loinc in ('2889-4') and a.result_num >= 300 and a.result_modifier in ('EQ', 'GE', 'GT'))		-- Proteinuria
or (a.lab_loinc in ('2890-2') and a.result_num >= 0.3 and a.result_modifier in ('EQ', 'GE', 'GT'))	-- Proteinuria
-- Proteinuria dipstick ??
or (a.lab_loinc in ('777-3') and a.result_num <= 99999 and a.result_modifier in ('EQ', 'LE', 'LT'))	-- Thrombocytopenia
or (a.lab_loinc in ('2160-0') and a.result_num >= 1.2 and a.result_modifier in ('EQ', 'GE', 'GT'))	-- Renal Insufficiency
)
,
lab_diagnosis as
(
select
    a.BIRTHID,
    min(b.lab_order_date) as 'earliest_lab_confirm_date',
    max(b.lab_order_date) as 'latest_lab_confirm_date'
from demographic_base a
left join lab_tmp b on a.motherid = b.patid and b.lab_order_date between a.preg_start_date and a.cutoff_time
group by a.birthid
)
SELECT
a.BIRTHID,
a.earliest_diagnosis_date,
a.latest_diagnosis_date,
b.earliest_lab_confirm_date,
b.latest_lab_confirm_date
FROM preeclampsia_diagnosis a
LEFT JOIN lab_diagnosis b ON a.BIRTHID = b.BIRTHID
ORDER BY a.BIRTHID;
'''

preeclampsia_sql_string = demographic_base_sql_string + ',' + '''
vital_tmp AS
(
SELECT
  a.BIRTHID,
  MEASURE_DATE + MEASURE_TIME AS 'measure_date',
  SYSTOLIC,
  DIASTOLIC,
  CASE WHEN (SYSTOLIC >= 160) OR (DIASTOLIC >= 110) THEN 2
	   WHEN (SYSTOLIC >= 140) OR (DIASTOLIC >= 90) THEN 1	
	   ELSE 0 END AS "bp_cat",
  CASE WHEN MEASURE_DATE < DATEADD(WEEK, 20, a.preg_start_date) THEN 0
       WHEN MEASURE_DATE BETWEEN DATEADD(WEEK, 20, a.preg_start_date) AND DATEADD(DAY, 90, a.preg_end_date) THEN 1 END AS "period"
FROM demographic_base a
INNER JOIN AMMI.dbo.VITAL b ON a.MOTHERID = b.PATID
 AND (b.SYSTOLIC IS NOT NULL AND b.DIASTOLIC IS NOT NULL AND MEASURE_DATE IS NOT NULL)
 AND b.MEASURE_DATE <= DATEADD(DAY, 90, a.preg_end_date)
)
,
-- the 4h criteria
vital_tmp_4h AS
(
SELECT
  BIRTHID,
  period,
  bp_cat
FROM vital_tmp
WHERE bp_cat > 0
GROUP BY BIRTHID, period, bp_cat
HAVING DATEDIFF(HOUR, MIN(measure_date), MAX(measure_date)) >= 4 
)
-- get the max bp_cat for each patient and period
,
bp_cat_tmp AS
(
SELECT
  BIRTHID,
  period,
  MAX(bp_cat) AS 'bp_cat'
FROM vital_tmp_4h
GROUP BY BIRTHID, period
)
,
-- the bp cat variable
bp_cat AS
(
SELECT
  a.BIRTHID,
  CASE WHEN b.bp_cat_before_20wk IS NULL THEN 0 ELSE b.bp_cat_before_20wk END AS 'bp_cat_before_20wk',
  CASE WHEN c.bp_cat_after_20wk IS NULL THEN 0 ELSE c.bp_cat_after_20wk END AS 'bp_cat_after_20wk'
FROM 
(SELECT DISTINCT BIRTHID FROM demographic_base) a
LEFT JOIN (SELECT BIRTHID, bp_cat AS 'bp_cat_before_20wk' FROM bp_cat_tmp WHERE period = 0) b 
  ON a.BIRTHID = b.BIRTHID
LEFT JOIN (SELECT BIRTHID, bp_cat AS 'bp_cat_after_20wk' FROM bp_cat_tmp WHERE period = 1) c
  ON a.BIRTHID = c.BIRTHID
)
,
-- chronic hypertension
chtn AS 
(
SELECT
  a.BIRTHID,
  CASE WHEN MAX(b.bp_cat_before_20wk) > 0 THEN 1 
       WHEN MAX(CASE WHEN DX_DATE IS NOT NULL THEN 1 ELSE 0 END) > 0 THEN 1 
	   ELSE 0 END AS 'chtn_any'
FROM demographic_base a
LEFT JOIN bp_cat b ON a.BIRTHID = b.BIRTHID
LEFT JOIN (SELECT * FROM AMMI.dbo.DIAGNOSIS WHERE DX LIKE 'I1%') c 
  ON a.MOTHERID = c.PATID AND c.DX_DATE < DATEADD(WEEK, 20, a.preg_start_date)
GROUP BY a.BIRTHID
)
,
-- magnisium infusion
mag_infuse AS
(
SELECT
  a.BIRTHID,
  MAX(CASE WHEN b.MEDADMIN_START_DATE IS NOT NULL THEN 1 ELSE 0 END) AS 'mag_infuse'
FROM demographic_base a
LEFT JOIN AMMI.dbo.MED_ADMIN b ON a.MOTHERID = b.PATID
 AND b.RAW_MEDADMIN_MED_NAME LIKE '%MAGNESIUM SULFATE%'
 AND b.MEDADMIN_START_DATE BETWEEN DATEADD(WEEK, 20, a.preg_start_date) AND DATEADD(DAY, 90, a.preg_end_date)
GROUP BY a.BIRTHID
)
,
-- pre-eclampsia with severe feature or eclampsia
icd_10_pre_sf AS
(
SELECT
  a.BIRTHID,
  MAX(CASE WHEN b.DX_DATE IS NOT NULL THEN 1 ELSE 0 END) AS 'pre_sf_or_ecl'
FROM demographic_base a
LEFT JOIN AMMI.dbo.DIAGNOSIS b ON a.MOTHERID = b.PATID
 AND (b.DX LIKE 'O14.1%' OR b.DX LIKE 'O14.2%' OR b.DX LIKE 'O15%')
 AND b.DX_DATE BETWEEN DATEADD(WEEK, 20, a.preg_start_date) AND DATEADD(DAY, 90, a.preg_end_date)
GROUP BY a.BIRTHID
)
,
-- pre-eclampsia with chronic hyptertension
icd_10_sipe AS
(
SELECT
  a.BIRTHID,
  MAX(CASE WHEN b.DX_DATE IS NOT NULL THEN 1 ELSE 0 END) AS 'sipe'
FROM demographic_base a
LEFT JOIN AMMI.dbo.DIAGNOSIS b ON a.MOTHERID = b.PATID
 AND b.DX LIKE 'O11%'
 AND b.DX_DATE BETWEEN DATEADD(WEEK, 20, a.preg_start_date) AND DATEADD(DAY, 90, a.preg_end_date)
GROUP BY a.BIRTHID
)
,
-- Proteinuria 
lab_proteinuria AS
(
SELECT
  a.BIRTHID,
  MAX(CASE WHEN b.LAB_ORDER_DATE IS NOT NULL THEN 1 ELSE 0 END) AS 'lab_proteinuria'
FROM demographic_base a
LEFT JOIN AMMI.dbo.LAB_RESULT_CM b ON a.MOTHERID = b.PATID
 AND b.LAB_ORDER_DATE BETWEEN DATEADD(WEEK, 20, a.preg_start_date) AND DATEADD(DAY, 90, a.preg_end_date)
 AND (
      (LAB_LOINC = '2889-4' AND -- protein in 24h urine > 300mg
      ((SUBSTRING(LOWER(RESULT_UNIT),1,2)='mg' AND RESULT_NUM >= 300) OR 
	   (SUBSTRING(LOWER(RESULT_UNIT),1,1)='g' AND RESULT_NUM >= 0.3))) 
   OR (LAB_LOINC = '2890-2' AND -- urine protein/creatine (UPC) > 0.3
      ((SUBSTRING(LOWER(RESULT_UNIT),1,2)='mg' AND RESULT_NUM >= 300) OR
	   (SUBSTRING(LOWER(RESULT_UNIT),1,2)='mg' AND NORM_RANGE_HIGH='<=0.15' AND RESULT_NUM >= 0.3) OR
	   (SUBSTRING(LOWER(RESULT_UNIT),1,1)='g' AND RESULT_NUM >= 0.3)))
)
GROUP BY a.BIRTHID
)
,
-- other lab
lab_others AS
(
SELECT
  a.BIRTHID,
  MAX(CASE WHEN b.LAB_ORDER_DATE IS NOT NULL THEN 1 ELSE 0 END) AS 'lab_others'
FROM demographic_base a
LEFT JOIN AMMI.dbo.LAB_RESULT_CM b ON a.MOTHERID = b.PATID
 AND b.LAB_ORDER_DATE BETWEEN DATEADD(WEEK, 20, a.preg_start_date) AND DATEADD(DAY, 90, a.preg_end_date)
 AND (
      (LAB_LOINC = '1920-8' AND RESULT_NUM >= 66) -- AST >= 66
   OR (LAB_LOINC = '1742-6' AND RESULT_NUM >= 72) -- ALT >= 72
   OR (LAB_LOINC = '2160-0' AND LOWER(RESULT_UNIT) = 'mg/dl' AND RESULT_NUM > 1.1 AND RESULT_NUM < 10) -- ct >1.1, there're too many lab with the same loinc code; there's also unit mL/min not sure what to do
   OR (LAB_LOINC = '777-3' AND SUBSTRING(RESULT_UNIT,1,2) = '10' AND RESULT_NUM < 100) -- plt < 100
)
GROUP BY a.BIRTHID
)
,
-- finally the decision table
decision_table AS
(
SELECT
  a.BIRTHID,
  a.pre_sf_or_ecl,
  b.mag_infuse,
  c.chtn_any,
  d.bp_cat_after_20wk,
  e.sipe,
  f.lab_proteinuria,
  g.lab_others
FROM icd_10_pre_sf a
LEFT JOIN mag_infuse b ON a.BIRTHID = b.BIRTHID
LEFT JOIN chtn c ON a.BIRTHID = c.BIRTHID
LEFT JOIN bp_cat d ON a.BIRTHID = d.BIRTHID
LEFT JOIN icd_10_sipe e ON a.BIRTHID = e.BIRTHID
LEFT JOIN lab_proteinuria f ON a.BIRTHID = f.BIRTHID
LEFT JOIN lab_others g ON a.BIRTHID = g.BIRTHID
)

SELECT 
  *,
  CASE WHEN (chtn_any = 0 AND (pre_sf_or_ecl = 1 OR bp_cat_after_20wk = 2 OR mag_infuse = 1 OR sipe = 1)) THEN 1  -- i think sipe can be 1 here
       WHEN (chtn_any = 0 AND bp_cat_after_20wk = 1 AND lab_others = 1) THEN 1 -- seems protienuria alone is not considered severe??
	   WHEN (chtn_any = 1 AND (pre_sf_or_ecl = 1 OR sipe = 1 OR mag_infuse = 1)) THEN 1
	   WHEN (chtn_any = 1 AND bp_cat_after_20wk = 2 AND lab_proteinuria = 1) THEN 1 -- this doesn't feel right to me, should include lab_others = 1??
	   WHEN (chtn_any = 1 AND bp_cat_after_20wk = 1 AND lab_others = 1) THEN 1
	   ELSE 0 END AS 'preeclampsia_label'
FROM decision_table
ORDER BY BIRTHID;
'''

get_rx_sql_string = demographic_base_sql_string + '''
SELECT
  a.BIRTHID,
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%aspirin%' THEN 1 ELSE 0 END) AS "med_rx_aspirin",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%nifedipine%' THEN 1 ELSE 0 END) AS "med_rx_nifedipine",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%insulin%' THEN 1 ELSE 0 END) AS "med_rx_insulin",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%amlodipine%' THEN 1 ELSE 0 END) AS "med_rx_amlodipine",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%glucagon%' THEN 1 ELSE 0 END) AS "med_rx_glucagon",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%glucose%' THEN 1 ELSE 0 END) AS "med_rx_glucose",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%methyldopa%' THEN 1 ELSE 0 END) AS "med_rx_methyldopa",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%promethazine%' THEN 1 ELSE 0 END) AS "med_rx_promethazine",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '% ethyl %' THEN 1 ELSE 0 END) AS "med_rx_ethyl",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%famotidine%' THEN 1 ELSE 0 END) AS "med_rx_famotidine",
  MAX(CASE WHEN LOWER(b.RAW_RX_MED_NAME) LIKE '%ondansetron%' THEN 1 ELSE 0 END) AS "med_rx_ondansetron"
FROM demographic_base a LEFT JOIN AMMI.dbo.PRESCRIBING b ON a.MOTHERID = b.PATID
 AND b.RX_ORDER_DATE BETWEEN a.preg_start_date AND a.cutoff_time
GROUP BY a.BIRTHID
ORDER BY a.BIRTHID
'''