Insert into AMMI.[dbo].BIRTH_RELATIONSHIP( [BIRTHID]
      ,[PREGNANCYID]
      ,[PATID]
      ,[ENCOUNTERID]
      ,[MOTHERID]
      ,[MOTHER_ENCOUNTERID])

 SELECT distinct
     wk.SUMMARY_BLOCK_ID as birthid
		,wk.PREG_EPISODE_ID as pregnancyid 
		,wk.BABY_PATIENT_NUM as patid
		,wk.BABY_ENCOUNTER_NUM as encounterid
		,wk.MOM_PATIENT_NUM as motherid
	  ,wk.[MOM_ENCOUNTER_NUM] as mother_encounterid	  
	
	 
FROM [AMMI].[dbo].[ammi_preg_pcornet__3_28_2024] wk 

join [AMMI].[dbo].DEMOGRAPHIC d2 on d2.patid = wk.baby_PATIENT_NUM
join [AMMI].[dbo].DEMOGRAPHIC d on d.patid = wk.MOM_PATIENT_NUM
join [AMMI].[dbo].ENCOUNTER e2 on e2.ENCOUNTERID = wk.BABY_ENCOUNTER_NUM 
join [AMMI].[dbo].ENCOUNTER e on e.ENCOUNTERID = wk.MOM_ENCOUNTER_NUM                             
where wk.BABY_PATIENT_NUM is not null 
