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
    
    --get patient_num for mom mrn
join patient_mapping_new@b2db wpi_mom on wpi_mom.PATIENT_IDE = pat_enc_mother.pat_id and wpi_mom.patient_ide_source = 'WAKE FOREST PAT_ID' 
and wpi_mom.SOURCESYSTEM_CD = 'WAKEONE'


--mapping; get encounter_num for del/mom CSN
 LEFT OUTER JOIN  phi_blind_enc@b2db pbe on pbe.encnter_idn = ob_hsb_delivery.delivery_date_csn and pbe.sourcesystem_cd != 'ATRIUM'
 LEFT OUTER JOIN  encounter_mapping@b2db em on pbe.enc_id = em.encounter_ide and em.sourcesystem_cd != 'ATRIUM' and em.PATIENT_IDE_SOURCE != 'ATRIUM'
 and em.ENCOUNTER_IDE_SOURCE != 'ATRIUM'

--mapping; get patient_num for baby mrn 
join patient_mapping_new@b2db wpi_baby on wpi_baby.PATIENT_IDE =  pat_enc_baby.pat_id and wpi_baby.patient_ide_source = 'WAKE FOREST PAT_ID' 
and wpi_baby.SOURCESYSTEM_CD = 'WAKEONE'

--mapping; get encounter_num for baby CSN 
 LEFT OUTER JOIN  phi_blind_enc@b2db pbe2 on pbe2.encnter_idn = ob_hsb_delivery.baby_birth_csn and pbe2.sourcesystem_cd != 'ATRIUM'
 LEFT OUTER JOIN encounter_mapping@b2db em2 on pbe2.enc_id = em2.encounter_ide and em2.sourcesystem_cd != 'ATRIUM' and em2.PATIENT_IDE_SOURCE != 'ATRIUM'
 and em2.ENCOUNTER_IDE_SOURCE != 'ATRIUM'
    
WHERE
    ob_hsb_delivery.ob_del_epis_type_c = 10
)

Insert into AMMI.[dbo].BIRTH_RELATIONSHIP( [BIRTHID]
      ,[PREGNANCYID]
      ,[PATID]
      ,[ENCOUNTERID]
      ,[MOTHERID]
      ,[MOTHER_ENCOUNTERID])

 SELECT distinct
 wk.SUMMARY_BLOCK_ID as birthid
		,wk.PREG_EPISODE_ID as pregnancyid 
		,wk.BABY_PATIENT_NUM as pat_id
		,wk.BABY_ENCOUNTER_NUM as encounterid
		,wk.MOM_PATIENT_NUM as motherid
	,wk.[MOM_ENCOUNTER_NUM] as mother_encounterid	  
	
	 
FROM patient_pop  wk 

join [AMMI].[dbo].DEMOGRAPHIC d2 on d2.patid = wk.baby_PATIENT_NUM
join [AMMI].[dbo].DEMOGRAPHIC d on d.patid = wk.MOM_PATIENT_NUM
join [AMMI].[dbo].ENCOUNTER e2 on e2.ENCOUNTERID = wk.BABY_ENCOUNTER_NUM 
join [AMMI].[dbo].ENCOUNTER e on e.ENCOUNTERID = wk.MOM_ENCOUNTER_NUM 
where wk.BABY_PATIENT_NUM is not null
