select
	birth_relationship.pregnancyid as pregnancy_id
	, birth_relationship.birthid as birth_id
	, birth_relationship.motherid as mother_id
	, birth_relationship.mother_encounterid as mother_delivery_encounter_id
	, birth_relationship.patid as baby_id
	, birth_relationship.encounterid as baby_delivery_encounter_id
	, demographic.birth_date as delivery_date
	, max(obs_clin.obsclin_result_num) as gestational_age_at_delivery
	, dateadd(DAY, -max(obs_clin.obsclin_result_num), demographic.birth_date) as estimated_pregnancy_start_date
	, datediff(YEAR, demographic_mother.birth_date, dateadd(DAY, -max(obs_clin.obsclin_result_num), demographic.birth_date) ) AS "mother_age_at_conception"
	, dateadd(DAY, 140, dateadd(DAY, -max(obs_clin.obsclin_result_num), demographic.birth_date)) as study_window_end_date
	, case when demographic_mother.hispanic = 'Y' then 1 else 0 end as is_hispanic
	, case when demographic_mother.race = '03' then 1 else 0 end as is_black

from
	{{ ref('birth_relationship') }}
	inner join {{ ref('demographic') }} on demographic.patid = birth_relationship.patid
		and demographic.birth_date is not null
	inner join {{ ref('obs_clin') }} on obs_clin.encounterid = birth_relationship.mother_encounterid
		and (obs_clin.obsclin_type in ('SM') and obs_clin.obsclin_code in ('444135009'))
inner join {{ ref('demographic') }} demographic_mother on demographic_mother.patid = birth_relationship.motherid
group by
	birth_relationship.pregnancyid
	, birth_relationship.birthid
	, birth_relationship.motherid
	, birth_relationship.mother_encounterid
	, birth_relationship.patid
	, birth_relationship.encounterid
	, demographic.birth_date
	, demographic_mother.hispanic
	, demographic_mother.race
	, demographic_mother.birth_date