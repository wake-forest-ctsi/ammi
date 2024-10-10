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
'''

age_sql_string = demographic_base_sql_string + '''
SELECT
  a.BIRTHID,
  a.gest_age_in_days,
  DATEDIFF(YEAR, b.BIRTH_DATE, a.preg_start_date) AS "mom_age"
FROM demographic_base a LEFT JOIN AMMI.dbo.DEMOGRAPHIC b ON a.MOTHERID = b.PATID
'''

blood_pressure_sql_string = demographic_base_sql_string + ',' + '''
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

preeclampsia_sql_string = demographic_base_sql_string + ',' + '''
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
'''