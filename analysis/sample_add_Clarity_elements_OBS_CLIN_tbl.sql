 ---########### Add Gestational Age and delivery Outcome to OBS_CLIN table #############
 with patient_pop as (
	SELECT 
		ob_hsb_delivery.summary_block_id  --birth_id
		,episode_del_rec.ob_del_preg_epi_id  as PREG_EPISODE_ID --pregnancy_id
		,COALESCE(ob_hsb_delivery.ob_del_birth_dttm, pat_baby.birth_date) as delivery_date
		,ob_hsb_delivery.baby_birth_csn as BABY_ENCOUNTER_ID --baby birth_csn
		,ob_hsb_delivery.delivery_date_csn as MOTHER_ENCOUNTER_ID --mother birth_csn
		, pat_enc_baby.pat_id  as BABY_PAT_ID --baby pat_id
		, pat_enc_mother.pat_id as MOM_PAT_ID --mother pat_id
		,wpi_mom.patient_num as MOM_PATIENT_NUM
		,wpi_baby.patient_num as BABY_PATIENT_NUM
		,em.encounter_num as mom_encounter_num    
		,em2.encounter_num as baby_encounter_num

		,PATIENT_3.PED_GEST_AGE_DAYS as gest_age_in_days
		,PATIENT_3.PED_GEST_AGE_NUM as gest_age_in_weeks
		,ZC_DELIVERY_TYPE.NAME AS DELIVERY_METHOD
		,ZC_OB_HX_OUTCOME.name as DELIVERY_OUTCOME
		,floor((pat_baby.birth_date - pat_mom.birth_date)/365.25) as mom_age_at_birth



	FROM 
		ob_hsb_delivery@clarity
		JOIN episode@clarity episode_del_rec ON ob_hsb_delivery.summary_block_id = episode_del_rec.episode_id
		JOIN episode@clarity episode_preg ON episode_del_rec.ob_del_preg_epi_id = episode_preg.episode_id
		JOIN pat_enc@clarity pat_enc_baby ON ob_hsb_delivery.baby_birth_csn = pat_enc_baby.pat_enc_csn_id
		JOIN pat_enc@clarity pat_enc_mother ON ob_hsb_delivery.delivery_date_csn = pat_enc_mother.pat_enc_csn_id
		JOIN patient@clarity pat_baby on pat_baby.pat_id = pat_enc_baby.pat_id
		JOIN patient@clarity pat_mom on pat_mom.pat_id = pat_enc_mother.pat_id

		LEFT OUTER JOIN PATIENT_3@clarityprod ON pat_enc_baby.pat_id = PATIENT_3.PAT_ID
		LEFT OUTER JOIN  ZC_DELIVERY_TYPE@clarityprod  ON ob_hsb_delivery.OB_DEL_DELIV_METH_C = ZC_DELIVERY_TYPE.DELIVERY_TYPE_C   
		LEFT OUTER JOIN ZC_OB_HX_OUTCOME@clarityprod on ZC_OB_HX_OUTCOME.OB_HX_OUTCOME_C = ob_hsb_delivery.ob_hx_outcome_c

		--get patient_num for mom mrn
	join patient_mapping_new@b2db wpi_mom on wpi_mom.PATIENT_IDE = pat_enc_mother.pat_id and wpi_mom.patient_ide_source = 'WAKE FOREST PAT_ID' 
	and wpi_mom.SOURCESYSTEM_CD = 'WAKEONE'


	--get encounter_num for del/mom CSN
	LEFT OUTER JOIN  phi_blind_enc@b2db pbe on pbe.encnter_idn = ob_hsb_delivery.delivery_date_csn and pbe.sourcesystem_cd != 'ATRIUM'
	LEFT OUTER JOIN  encounter_mapping@b2db em on pbe.enc_id = em.encounter_ide and em.sourcesystem_cd != 'ATRIUM' and em.PATIENT_IDE_SOURCE != 'ATRIUM'
	and em.ENCOUNTER_IDE_SOURCE != 'ATRIUM'

	--get patient_num for baby mrn
	join patient_mapping_new@b2db wpi_baby on wpi_baby.PATIENT_IDE =  pat_enc_baby.pat_id and wpi_baby.patient_ide_source = 'WAKE FOREST PAT_ID' 
	and wpi_baby.SOURCESYSTEM_CD = 'WAKEONE'

	--get encounter_num for baby CSN
	LEFT OUTER JOIN  phi_blind_enc@b2db pbe2 on pbe2.encnter_idn = ob_hsb_delivery.baby_birth_csn and pbe2.sourcesystem_cd != 'ATRIUM'
	LEFT OUTER JOIN encounter_mapping@b2db em2 on pbe2.enc_id = em2.encounter_ide and em2.sourcesystem_cd != 'ATRIUM' and em2.PATIENT_IDE_SOURCE != 'ATRIUM'
	and em2.ENCOUNTER_IDE_SOURCE != 'ATRIUM'

	WHERE
		ob_hsb_delivery.ob_del_epis_type_c = 10

)
 
---########### Add Gestational Age #############
INSERT INTO [AMMI].[dbo].[OBS_CLIN](
	     [OBSCLINID]
      ,[PATID]
      ,[ENCOUNTERID]
      ,[OBSCLIN_PROVIDERID]
      ,[OBSCLIN_START_DATE]
      ,[OBSCLIN_START_TIME]
      ,[OBSCLIN_STOP_DATE]
      ,[OBSCLIN_STOP_TIME]
      ,[OBSCLIN_TYPE]
      ,[OBSCLIN_CODE]
      ,[OBSCLIN_RESULT_QUAL]
      ,[OBSCLIN_RESULT_TEXT]
      ,[OBSCLIN_RESULT_SNOMED]
      ,[OBSCLIN_RESULT_NUM]
      ,[OBSCLIN_RESULT_MODIFIER]
      ,[OBSCLIN_RESULT_UNIT]
      ,[OBSCLIN_SOURCE]
      ,[OBSCLIN_ABN_IND]
      ,[RAW_OBSCLIN_NAME]
      ,[RAW_OBSCLIN_CODE]
      ,[RAW_OBSCLIN_TYPE]
      ,[RAW_OBSCLIN_RESULT]
      ,[RAW_OBSCLIN_MODIFIER]
      ,[RAW_OBSCLIN_UNIT]
)

select distinct
		--[OBSCLINID]
	ROW_NUMBER() OVER (order by (select 1))+ (SELECT COALESCE(MAX(OBSCLINID),0) FROM [AMMI].[dbo].[OBS_CLIN])
	--      [PATID]
	,e.PATID
	--      [ENCOUNTERID]
	,e.ENCOUNTERID
	--      [OBSCLIN_PROVIDERID]
	,e.[PROVIDERID]
	--      [OBSCLIN_START_DATE]
	,e.[ADMIT_DATE]
	--      [OBSCLIN_START_TIME]
	,e.[ADMIT_TIME]
	--      [OBSCLIN_STOP_DATE]
	,e.[DISCHARGE_DATE]
	--      [OBSCLIN_STOP_TIME]
	,e.[DISCHARGE_TIME]
	--      [OBSCLIN_TYPE]
	,'SM'
	--      [OBSCLIN_CODE]
	, '444135009'
	--      [OBSCLIN_RESULT_QUAL]
	,'NI'
	--      [OBSCLIN_RESULT_TEXT]
	, NULL
	--      [OBSCLIN_RESULT_SNOMED]
	,NULL
	--      [OBSCLIN_RESULT_NUM]
	,a.[GEST_AGE_IN_DAYS]
	--      [OBSCLIN_RESULT_MODIFIER]
	,'EQ'
	--      [OBSCLIN_RESULT_UNIT]
	,'days'
	--      [OBSCLIN_SOURCE]
	,'HC'
	--      [OBSCLIN_ABN_IND]
	,'NI'
	--      [RAW_OBSCLIN_NAME]
	,'Estimated fetal gestational age at delivery'
	--      [RAW_OBSCLIN_CODE]
	,'Estimated fetal gestational age at delivery'
	--      [RAW_OBSCLIN_TYPE]
	,NULL
	--      [RAW_OBSCLIN_RESULT]
	,a.[GEST_AGE_IN_DAYS]
	--      [RAW_OBSCLIN_MODIFIER]
	,NULL
	--      [RAW_OBSCLIN_UNIT]
	,'days'

from 
patient_pop a
join [AMMI].dbo.encounter e on e.[ENCOUNTERID] = a.MOM_ENCOUNTER_NUM
;

---Comment out INSERT statement above to run this part --
--####Insert DELIVERY_OUTCOME into [OBS_CLIN] table ####
INSERT INTO [AMMI].[dbo].[OBS_CLIN](
[OBSCLINID]
      ,[PATID]
      ,[ENCOUNTERID]
      ,[OBSCLIN_PROVIDERID]
      ,[OBSCLIN_START_DATE]
      ,[OBSCLIN_START_TIME]
      ,[OBSCLIN_STOP_DATE]
      ,[OBSCLIN_STOP_TIME]
      ,[OBSCLIN_TYPE]
      ,[OBSCLIN_CODE]
      ,[OBSCLIN_RESULT_QUAL]
      ,[OBSCLIN_RESULT_TEXT]
      ,[OBSCLIN_RESULT_SNOMED]
      ,[OBSCLIN_RESULT_NUM]
      ,[OBSCLIN_RESULT_MODIFIER]
      ,[OBSCLIN_RESULT_UNIT]
      ,[OBSCLIN_SOURCE]
      ,[OBSCLIN_ABN_IND]
      ,[RAW_OBSCLIN_NAME]
      ,[RAW_OBSCLIN_CODE]
      ,[RAW_OBSCLIN_TYPE]
      ,[RAW_OBSCLIN_RESULT]
      ,[RAW_OBSCLIN_MODIFIER]
      ,[RAW_OBSCLIN_UNIT]
)

select distinct
	 --[OBSCLINID]
	ROW_NUMBER() OVER (order by (select 1))+ (SELECT COALESCE(MAX(OBSCLINID),0) FROM [AMMI].[dbo].[OBS_CLIN])
	-- [PATID]
	,e.PATID
	--      [ENCOUNTERID]
	,e.ENCOUNTERID
	--      [OBSCLIN_PROVIDERID]
	,e.[PROVIDERID]
	--      [OBSCLIN_START_DATE]
	,e.[ADMIT_DATE]
	--      [OBSCLIN_START_TIME]
	,e.[ADMIT_TIME]
	--      [OBSCLIN_STOP_DATE]
	,e.[DISCHARGE_DATE]
	--      [OBSCLIN_STOP_TIME]
	,e.[DISCHARGE_TIME]

	--      [OBSCLIN_TYPE]
	,'SM'

	--      [OBSCLIN_CODE]
  ,  CASE
			when  a.[DELIVERY_OUTCOME] = 'Current'  then  'Unknown'
			when  a.[DELIVERY_OUTCOME] = 'Term'  then  '21243004'
			when  a.[DELIVERY_OUTCOME] = 'Preterm'  then  '282020008'

			when  a.[DELIVERY_OUTCOME] = 'Abortion'  then  '386639001'
			when  a.[DELIVERY_OUTCOME] = 'Gravida'  then  'Unknown'
			when  a.[DELIVERY_OUTCOME] = 'Para'  then  'Unknown'

			when  a.[DELIVERY_OUTCOME] = 'Induced Abortion'  then  '57797005'

			when  a.[DELIVERY_OUTCOME] = 'Spontaneous Abortion'  then  '17369002'

			when  a.[DELIVERY_OUTCOME] = 'Ectopic'  then  'Unknown'
			when  a.[DELIVERY_OUTCOME] = 'Molar'  then  'Unknown'			
			when a.[DELIVERY_OUTCOME]  is NULL then 'Unknown'
	   END

	-- [OBSCLIN_RESULT_QUAL]
	,'NI'
	--      [OBSCLIN_RESULT_TEXT]
	,a.[DELIVERY_OUTCOME]

	--      [OBSCLIN_RESULT_SNOMED]
	,NULL
	--      [OBSCLIN_RESULT_NUM]
	,NULL
	--      [OBSCLIN_RESULT_MODIFIER]
	,'EQ'
	--      [OBSCLIN_RESULT_UNIT]
	,NULL
	--      [OBSCLIN_SOURCE]
	,'HC'
	--      [OBSCLIN_ABN_IND]
	,'NI'
	--      [RAW_OBSCLIN_NAME]
	,  CASE
				when  a.[DELIVERY_OUTCOME] = 'Current'  then  'Unknown'
				when  a.[DELIVERY_OUTCOME] = 'Term'  then  'Term birth of newborn'
				when  a.[DELIVERY_OUTCOME] = 'Preterm'  then  'Premature delivery'
				when  a.[DELIVERY_OUTCOME] = 'Abortion'  then  'Abortion'
				when  a.[DELIVERY_OUTCOME] = 'Gravida'  then  'Unknown'
				when  a.[DELIVERY_OUTCOME] = 'Para'  then  'Unknown'
				when  a.[DELIVERY_OUTCOME] = 'Induced Abortion'  then  'Induced Abortion'
				when  a.[DELIVERY_OUTCOME] = 'Spontaneous Abortion'  then  'Spontaneous Abortion'
				when  a.[DELIVERY_OUTCOME] = 'Ectopic'  then  'Unknown'
				when  a.[DELIVERY_OUTCOME] = 'Molar'  then  'Unknown'			
				when a.[DELIVERY_OUTCOME]  is NULL then 'Unknown'
		END


	--      [RAW_OBSCLIN_CODE]
	, null

	--      [RAW_OBSCLIN_TYPE]
	,NULL

	--      [RAW_OBSCLIN_RESULT]
	, CASE
				when  a.[DELIVERY_OUTCOME] = 'Current'  then  'Current'
				when  a.[DELIVERY_OUTCOME] = 'Term'  then  'Term'
				when  a.[DELIVERY_OUTCOME] = 'Preterm'  then  'Preterm'
				when  a.[DELIVERY_OUTCOME] = 'Abortion'  then  'Abortion'
				when  a.[DELIVERY_OUTCOME] = 'Gravida'  then  'Gravida'
				when  a.[DELIVERY_OUTCOME] = 'Para'  then  'Para'
				when  a.[DELIVERY_OUTCOME] = 'Induced Abortion'  then  'Induced Abortion'
				when  a.[DELIVERY_OUTCOME] = 'Spontaneous Abortion'  then  'Spontaneous Abortion'
				when  a.[DELIVERY_OUTCOME] = 'Ectopic'  then  'Ectopic'
				when  a.[DELIVERY_OUTCOME] = 'Molar'  then  'Molar'			
				when a.[DELIVERY_OUTCOME]  is NULL then 'Unknown'
		END

	--      [RAW_OBSCLIN_MODIFIER]
	,NULL
	--      [RAW_OBSCLIN_UNIT]
	,NULL

from 
patient_pop a
join [AMMI].dbo.encounter e on e.[ENCOUNTERID] = a.MOM_ENCOUNTER_NUM
