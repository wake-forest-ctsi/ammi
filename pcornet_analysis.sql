/* Birth episodes with a preeclampsia phenotype */
with pregnancy_episodes as
(
	select
		encounter.patid,
		birth_relationship.birthid,
		dateadd(MONTH,-9,encounter.admit_date) as episode_begin_datetime,
		encounter.admit_date as episode_end_datetime
	from
		birth_relationship
		join encounter on encounter.encounterid = birth_relationship.mother_encounterid
),
pregnancy_vitals as
(
	select
		pregnancy_episodes.*,
		vital.systolic,
		vital.diastolic,
		vital.measure_date + vital.measure_time as measure_datetime
	from vital 
	join pregnancy_episodes on 
		vital.patid = pregnancy_episodes.patid
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
		hdp_vitals_rank.measure_datetime_b
		--,hdp_vitals_rank.severe_hypertension
	from
		(select
			row_number() over (partition by hdp_vitals.birthid order by hdp_vitals.severe_hypertension desc, hdp_vitals.hypertension_score desc) as rownum,
			hdp_vitals.*
		from
			hdp_vitals) hdp_vitals_rank
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
		lab_result_cm.specimen_date + lab_result_cm.specimen_time as specimen_datetime,
		case when lab_result_cm.lab_loinc in ('2889-4') then 1
			when lab_result_cm.lab_loinc in ('2890-2') then 2
			when lab_result_cm.lab_loinc in ('777-3') then 3
			when lab_result_cm.lab_loinc in ('2160-0') then 4
		end as preeclampsia_features
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
),
hdp_labs_max as (
		select
		hdp_labs_rank.patid,
		hdp_labs_rank.birthid,
		hdp_labs_rank.episode_begin_datetime,
		hdp_labs_rank.episode_end_datetime,
		hdp_labs_rank.lab_loinc,
		hdp_labs_rank.result_num,
		hdp_labs_rank.result_modifier,
		hdp_labs_rank.specimen_datetime
	from
		(select
			row_number() over (partition by hdp_labs.birthid order by hdp_labs.preeclampsia_features asc) as rownum,
			hdp_labs.*
		from
			hdp_labs) hdp_labs_rank
	where
		hdp_labs_rank.rownum = 1
	
)
select
	hdp_vitals_max.*,
	hdp_labs_max.lab_loinc,
	hdp_labs_max.result_num,
	hdp_labs_max.result_modifier,
	hdp_labs_max.specimen_datetime
from
	hdp_vitals_max
	inner join hdp_labs_max on hdp_labs_max.birthid = hdp_vitals_max.birthid
;

/* Birth episodes with a preeclampsia diagnosis */
with pregnancy_episodes as
(
	select
		encounter.patid,
		birth_relationship.birthid,
		dateadd(MONTH,-9,encounter.admit_date) as episode_begin_datetime,
		encounter.admit_date as episode_end_datetime
	from
		birth_relationship
		join encounter on encounter.encounterid = birth_relationship.mother_encounterid
)
select 
	pregnancy_episodes.*,
	diagnosis.dx_type,
	diagnosis.dx,
	diagnosis.dx_date
from 
	pregnancy_episodes
	inner join diagnosis on diagnosis.dx_date between pregnancy_episodes.episode_begin_datetime and pregnancy_episodes.episode_end_datetime
where
	diagnosis.dx_type = '10' and diagnosis.dx like 'O14%'
;
