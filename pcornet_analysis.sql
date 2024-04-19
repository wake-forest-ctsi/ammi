-- ACOG defines HDP as two values at least four hours apart. We use MEASURE_TAKEN_INSTANT for elevated blood pressures to identify the first and last blood pressure that meets criteria:
-- CASE WHEN eb.sbp_value >= 140 OR eb.DBP_VALUE >=90 THEN eb.MEASURE_TAKEN_INSTANT ELSE NULL END) AS mild_bp_first

-- Discrete lab values that are used to diagnose preeclampsia include Platelet count, AST, ALT, Creatinine, 24 hour urine protein, and urine protein:creatinine ratio. 

-- Using lab_component_key values, we retrieved the minimum or maximum value for each lab. Platelets <100,000 are diagnostic of preeclampsia; AST, ALT, Creatinine, and urine protein are diagnostic if they exceed thresholds

with pregnancy_episodes as
(
	select
		encounter.patid,
		birth_relationship.birthid,
		dateadd(MONTH,-9,encounter.admit_date) as episode_begin_datetime,
		encounter.admit_date as episode_end_datetime
	from
		BIRTH_RELATIONSHIP
		join encounter on encounter.ENCOUNTERID = birth_relationship.MOTHER_ENCOUNTERID
),
pregnancy_vitals as
(
	select
		pregnancy_episodes.*,
		vital.SYSTOLIC,
		vital.DIASTOLIC,
		vital.MEASURE_DATE + vital.MEASURE_TIME as measure_datetime
	from vital 
	join pregnancy_episodes on 
		vital.PATID = pregnancy_episodes.PATID
		and (vital.measure_date + vital.measure_time) between episode_begin_datetime and episode_end_datetime
),
hdp_vitals as
(
	select
		a.patid,
		a.birthid,
		a.episode_begin_datetime,
		a.episode_end_datetime,
		a.systolic as systolic_a,
		a.diastolic as diastolic_a,
		a.measure_datetime as measure_datetime_a,
		b.systolic as systolic_b,
		b.diastolic as diastolic_b,
		b.measure_datetime as measure_datetime_b,
		case when a.systolic >= 160 and b.systolic >= 160 and a.diastolic >= 110 and b.diastolic >= 110 then 1 else 0 end severe_hypertension,
		(a.systolic - 140) + (b.systolic - 140) + (a.diastolic - 90) + (b.diastolic - 90) as hypertension_score
	from
		pregnancy_vitals a
		join pregnancy_vitals b on
			a.birthid = b.birthid 
			and b.measure_datetime > (a.measure_datetime + '04:00')
			and (b.systolic >= 140 or b.diastolic >= 90)
	where
		a.systolic >= 140
		or a.diastolic >= 90
),
rank_hdp_vitals as
(
	select
		row_number() over (partition by hdp_vitals.birthid order by hdp_vitals.severe_hypertension desc, hdp_vitals.hypertension_score desc) as rownum,
		hdp_vitals.*
	from
		hdp_vitals
),
max_hdp_vitals as
(
	select
		rank_hdp_vitals.patid,
		rank_hdp_vitals.birthid,
		rank_hdp_vitals.episode_begin_datetime,
		rank_hdp_vitals.episode_end_datetime,
		rank_hdp_vitals.systolic_a,
		rank_hdp_vitals.diastolic_a,
		rank_hdp_vitals.measure_datetime_a,
		rank_hdp_vitals.systolic_b,
		rank_hdp_vitals.diastolic_b,
		rank_hdp_vitals.measure_datetime_b,
		rank_hdp_vitals.severe_hypertension
	from
		rank_hdp_vitals
	where
		rank_hdp_vitals.rownum = 1
)
select
	*
from
	max_hdp_vitals
;