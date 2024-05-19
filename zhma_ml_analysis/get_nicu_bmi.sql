-- get baby gest_age, mums age, pregnancy period
-- try not to drop anything at this table, everything is going to join this cte
WITH demographic_base AS
(
SELECT
  a.BIRTHID,
  a.PATID,
  a.ENCOUNTERID,
  a.PREGNANCYID,
  a.MOTHERID,
  a.MOTHER_ENCOUNTERID,
  b.BIRTH_DATE,
  c.gest_age_in_days,
  c.obs_start_date,
  (CASE WHEN b.BIRTH_DATE IS NOT NULL THEN b.BIRTH_DATE - c.gest_age_in_days -- about 4 is null in birth_date, sigh
   ELSE c.obs_start_date - c.gest_age_in_days END) AS "preg_start_date",
  (CASE WHEN b.BIRTH_DATE IS NOT NULL THEN b.BIRTH_DATE ELSE c.obs_start_date END) AS "preg_end_date",
  d.BIRTH_DATE AS "mother_birth_date"
FROM AMMI.dbo.BIRTH_RELATIONSHIP a
  LEFT JOIN AMMI.dbo.DEMOGRAPHIC b ON a.PATID = b.PATID
  LEFT JOIN 
  (SELECT 
    ENCOUNTERID,
    MIN(OBSCLIN_RESULT_NUM) AS "gest_age_in_days", -- it affects about 4 records of twins with different gest_age
	MIN(OBSCLIN_START_DATE) AS "obs_start_date"
   FROM AMMI.dbo.OBS_CLIN
   WHERE RAW_OBSCLIN_NAME = 'Estimated fetal gestational age at delivery'
   GROUP BY ENCOUNTERID) c ON a.MOTHER_ENCOUNTERID = c.ENCOUNTERID
  LEFT JOIN AMMI.dbo.DEMOGRAPHIC d on a.MOTHERID = d.PATID
)
,
-- get the nicu admission for the baby
-- nicu codes given by dr holman, used bewteen '99466' AND '99480' earlier
nicu_stats AS
(
SELECT 
  a.PATID,
  MAX(CASE WHEN b.PX IS NULL THEN NULL        -- it can null here
           WHEN b.PX IN ('99468','99471','99477') THEN 1
		   ELSE 0 END) AS "nicu_admission" 
FROM demographic_base a
--LEFT JOIN AMMI.dbo.[PROCEDURES] b ON a.ENCOUNTERID = b.ENCOUNTERID
LEFT JOIN AMMI.dbo.[PROCEDURES] b ON a.PATID = b.PATID  -- sometimes the baby will get into nicu after the delivery encounter
GROUP BY a.PATID
)
,
-- get bmi if exists during the first 12 weeks (84 days) of pregnancy
-- well, if we only ask for minimal or average bmi, we can replace the window function with MIN
bmi_stats AS
(
SELECT PATID, earliest_bmi FROM 
(SELECT
  a.PATID,
  b.OBSCLIN_RESULT_NUM AS "earliest_bmi",
  ROW_NUMBER() OVER (PARTITION BY a.PATID ORDER BY b.OBSCLIN_START_DATE) AS "k"  -- get the earliest bmi
FROM demographic_base a 
LEFT JOIN (SELECT * FROM AMMI.dbo.OBS_CLIN WHERE RAW_OBSCLIN_CODE = 'VITAL:ORIGINAL_BMI') b 
  ON (a.MOTHERID = b.PATID) AND (b.OBSCLIN_START_DATE BETWEEN a.preg_start_date AND a.preg_start_date + 84)
) tmp 
WHERE k = 1
)
,
-- get the weight and height to recover null bmi (about ~300 is recovered)
weight_stats AS
(
SELECT PATID, earliest_weight FROM 
(SELECT
  a.PATID,
  b.OBSCLIN_RESULT_NUM AS "earliest_weight",
  ROW_NUMBER() OVER (PARTITION BY a.PATID ORDER BY b.OBSCLIN_START_DATE) AS "k"  -- get the earliest weight
FROM demographic_base a 
LEFT JOIN (SELECT * FROM AMMI.dbo.OBS_CLIN WHERE RAW_OBSCLIN_CODE = 'VITAL:WT') b 
  ON (a.MOTHERID = b.PATID) AND (b.OBSCLIN_START_DATE BETWEEN a.preg_start_date AND a.preg_start_date + 84)
) tmp 
WHERE k = 1
)
,
-- get the max height
height_stats AS
(
SELECT
  a.PATID,
  MAX(b.OBSCLIN_RESULT_NUM) AS "max_height"
FROM demographic_base a
LEFT JOIN (SELECT * FROM AMMI.dbo.OBS_CLIN WHERE RAW_OBSCLIN_CODE = 'VITAL:HT') b ON a.MOTHERID = b.PATID
GROUP BY a.PATID
)

-- now join every one with the demographic_base
SELECT 
  a.*,
  b.earliest_weight,
  c.max_height,
  d.earliest_bmi,
  b.earliest_weight / SQUARE(c.max_height) * 703 AS "computed_bmi",
  e.nicu_admission
FROM demographic_base a
LEFT JOIN weight_stats b ON a.PATID = b.PATID
LEFT JOIN height_stats c ON a.PATID = c.PATID
LEFT JOIN bmi_stats d ON a.PATID = d.PATID
LEFT JOIN nicu_stats e ON a.PATID = e.PATID
ORDER BY preg_end_date;