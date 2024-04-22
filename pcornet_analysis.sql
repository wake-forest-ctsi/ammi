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
		vital.PATID = pregnancy_episodes.patid
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
		(a.systolic - 140) + (b.systolic - 140) + (a.diastolic - 90) + (b.diastolic - 90) as hypertension_score  --
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
hdp_vitals_rank as
(
	select
		row_number() over (partition by hdp_vitals.birthid order by hdp_vitals.severe_hypertension desc, hdp_vitals.hypertension_score desc) as rownum,
		hdp_vitals.*
	from
		hdp_vitals
),
hdp_vitals_max as
(
	select
		hdp_vitals_rank.patid,
		hdp_vitals_rank.birthid,
		hdp_vitals_rank.episode_begin_datetime,
		hdp_vitals_rank.episode_end_datetime,
		hdp_vitals_rank.systolic_a,
		hdp_vitals_rank.diastolic_a,
		hdp_vitals_rank.measure_datetime_a,
		hdp_vitals_rank.systolic_b,
		hdp_vitals_rank.diastolic_b,
		hdp_vitals_rank.measure_datetime_b,
		hdp_vitals_rank.severe_hypertension
	from
		hdp_vitals_rank
	where
		hdp_vitals_rank.rownum = 1
),
hdp_labs as
(
	select
		pregnancy_episodes.*,
		lab_result_cm.lab_loinc,
		lab_result_cm.result_num,
		lab_result_cm.result_modifier,
		lab_result_cm.specimen_date + lab_result_cm.specimen_time as specimen_datetime
	from lab_result_cm
	join pregnancy_episodes on 
		lab_result_cm.PATID = pregnancy_episodes.patid
		and (lab_result_cm.specimen_date + lab_result_cm.specimen_time) between episode_begin_datetime and episode_end_datetime
		and 
			( 
				(lab_result_cm.lab_loinc in ('2889-4') and lab_result_cm.result_num >= 300 and lab_result_cm.result_modifier in ('EQ', 'GE', 'GT'))		-- Proteinuria
				or (lab_result_cm.lab_loinc in ('2890-2') and lab_result_cm.result_num >= 0.3 and lab_result_cm.result_modifier in ('EQ', 'GE', 'GT'))	-- Proteinuria
				-- Proteinuria dipstick ??
				or (lab_result_cm.lab_loinc in ('777-3') and lab_result_cm.result_num <= 99999 and lab_result_cm.result_modifier in ('EQ', 'LE', 'LT'))	-- Thrombocytopenia
				or (lab_result_cm.lab_loinc in ('2160-0') and lab_result_cm.result_num >= 1.2 and lab_result_cm.result_modifier in ('EQ', 'GE', 'GT'))	-- Renal Insufficiency
					-- or a doubling of the serum creatinine concentration in the absence of other renal disease?
				-- Impaired liver function?
				-- Pulmonary Edema?
				-- New onset headache?
			)
)
select
	hdp_vitals_max.*
	, hdp_labs.*
from
	hdp_vitals_max
	inner join hdp_labs on hdp_labs.birthid = hdp_vitals_max.birthid
;