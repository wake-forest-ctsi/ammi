						
						
	DROP TABLE "ASTUEBE".AMMI_PREG_01;					
	CREATE TABLE "ASTUEBE".AMMI_PREG_01 AS 					
	( SELECT DISTINCT  pm.PATIENT_KEY AS mom_pt_key,					
	ids.preg_id,					
	ids.mom_mrn,					
	max(ids.mom_del_enc) AS mom_preg_del_enc,					
	max(ids.del_date_time) AS preg_del_date_time,					
	max(ids.del_date) AS preg_del_date,					
	max (ids.BABY_GA_BIRTH_W) AS preg_ga_wk					
	FROM  U223355."ids01_del_new" ids					
	LEFT JOIN _SYS_BIC."CDW.Reporting_View/PATIENT_MASTER" pm ON (ids.mom_mrn = pm.MEDICAL_RECORD_NUMBER ) 					
	GROUP BY  pm.PATIENT_KEY, ids.preg_id, ids.mom_mrn					
	)					
						
						
	DROP TABLE "ASTUEBE".ammi_del_bpcat_01;					
						
	CREATE TABLE "ASTUEBE".ammi_del_bpcat_01 AS ( 					
	WITH bp_data AS (					
		WITH enc_bp_01 AS (				
			SELECT			
				mom_fs.PATIENT_KEY,		
				mom_fs.ENCOUNTER_KEY ,		
				mom_fs.measure_taken_instant,		
				mom_fs.measure_value	,	
				CAST (substr_regexpr('(\d{1,3})\/(\d{1,3})' IN mom_fs.measure_value GROUP 1) AS int) AS sbp_value,		
				CAST (substr_regexpr('(\d{1,3})\/(\d{1,3})' IN mom_fs.measure_value GROUP 2) AS int) AS dbp_value		
						
			FROM			
				ASTUEBE.AMMI_PREG_01 enc		
			LEFT JOIN _SYS_BIC."CDW.Reporting_View/ENCOUNTER_TO_FLOWSHEET_MASTER" mom_fs ON			
				(mom_fs.encounter_key = enc.MOM_PREG_DEL_ENC )		
			WHERE			
				( upper(mom_fs.flowsheet_row_name) LIKE '%BLOOD PRESSURE%')		
			ORDER BY			
				mom_fs.PATIENT_KEY,		
				mom_fs.measure_taken_instant		
			)			
		SELECT				
		eb.ENCOUNTER_KEY,				
		max (eb.SBP_VALUE) AS enc_max_sbp,				
		max(eb.DBP_VALUE) AS enc_max_dbp,				
		max (CASE				
			WHEN eb.SBP_VALUE > 140			
				OR eb.DBP_VALUE > 90 THEN 1		
				ELSE 0		
			END) AS enc_cat_bp_mild,			
		max (CASE				
			WHEN eb.SBP_VALUE > 160			
				OR eb.DBP_VALUE > 110 THEN 1		
				ELSE 0		
			END) AS enc_cat_bp_severe,			
		max (CASE				
			WHEN eb.SBP_VALUE > 160			
				OR eb.DBP_VALUE > 110 THEN 3		
				WHEN eb.SBP_VALUE > 150		
				OR eb.DBP_VALUE > 100 THEN 2		
				WHEN eb.SBP_VALUE > 140		
				OR eb.DBP_VALUE > 90 THEN 1		
				ELSE 0	END) AS enc_maxbp_cat	,
		min (CASE WHEN eb.sbp_value >= 140 OR eb.DBP_VALUE >=90 THEN eb.MEASURE_TAKEN_INSTANT ELSE NULL END) AS mild_bp_first,				
		max (CASE WHEN eb.sbp_value >= 140 OR eb.DBP_VALUE >=90 THEN eb.MEASURE_TAKEN_INSTANT ELSE NULL END) AS mild_bp_last,				
		min (CASE WHEN eb.sbp_value >= 150 OR eb.DBP_VALUE >=100 THEN eb.MEASURE_TAKEN_INSTANT ELSE NULL END) AS mod_bp_first,				
		max (CASE WHEN eb.sbp_value >= 150 OR eb.DBP_VALUE >=100 THEN eb.MEASURE_TAKEN_INSTANT ELSE NULL END) AS mod_bp_last,				
		min (CASE WHEN eb.sbp_value >= 160 OR eb.DBP_VALUE >=110 THEN eb.MEASURE_TAKEN_INSTANT ELSE NULL END) AS sev_bp_first,				
		max (CASE WHEN eb.sbp_value >= 160 OR eb.DBP_VALUE >=110 THEN eb.MEASURE_TAKEN_INSTANT ELSE NULL END) AS sev_bp_last				
		FROM				
			ENC_BP_01 eb			
		GROUP BY				
			eb.ENCOUNTER_KEY			
		)				
	SELECT bpd.ENCOUNTER_KEY,					
	bpd.enc_max_sbp,					
	bpd.enc_max_dbp, 					
	bpd.enc_cat_bp_mild,  					
	bpd.enc_cat_bp_severe,					
	bpd.enc_maxbp_cat,					
	CASE WHEN  (CAST(seconds_between(bpd.mild_bp_first,bpd.mild_bp_last)/60 AS int) > 240 ) THEN 1 ELSE 0 END AS enc_htn_mild,					
	CASE WHEN  (CAST(seconds_between(bpd.mod_bp_first,bpd.mod_bp_last)/60 AS int) > 240 ) THEN 1 ELSE 0 END AS enc_htn_mod,					
	CASE WHEN  (CAST(seconds_between(bpd.sev_bp_first,bpd.sev_bp_last)/60 AS int) > 15 ) THEN 1 ELSE 0 END AS enc_htn_sev,					
	CASE WHEN  (CAST(seconds_between(bpd.sev_bp_first,bpd.sev_bp_last)/60 AS int) > 15 ) THEN 3					
		WHEN (CAST(seconds_between(bpd.mod_bp_first,bpd.mod_bp_last)/60 AS int) > 240 ) THEN 2				
		WHEN (CAST(seconds_between(bpd.mild_bp_first,bpd.mild_bp_last)/60 AS int) > 240 ) THEN 1				
		ELSE 0 END AS enc_htn_cat				
						
	FROM bp_data bpd );					
						
						
						
						
-- code to create birth hospitalization HDP table						
	DROP TABLE ASTUEBE.ammi_HDP_01;					
	CREATE TABLE ASTUEBE.ammi_HDP_01 AS (					
			 WITH birthenc AS (			
			 	SELECT DISTINCT 		
			 	ids.MOM_PREG_DEL_ENC ,		
				MAX ( CASE WHEN idsn.MOM_DIAG_ICD10_HB_ALL LIKE '%O14.1%' 		
					OR idsn.MOM_DIAG_ICD10_HB_ALL LIKE '%O14.2%' 	
					OR idsn.MOM_DIAG_ICD10_PB_ALL LIKE '%O14.1%'	
					OR idsn.MOM_DIAG_ICD10_PB_ALL LIKE '%O14.2%' THEN 1 ELSE 0 END ) AS dx_icd10_pre_sf,	
				MAX ( CASE WHEN idsn.MOM_DIAG_ICD10_HB_ALL LIKE '%O14.0%' 		
					OR idsn.MOM_DIAG_ICD10_HB_ALL LIKE '%O14.9%' 	
					OR idsn.MOM_DIAG_ICD10_PB_ALL LIKE '%O14.0%'	
					OR idsn.MOM_DIAG_ICD10_PB_ALL LIKE '%O14.9%' THEN 1 ELSE 0 END ) AS dx_icd10_pre_nosf,	
				MAX ( CASE WHEN idsn.MOM_DIAG_ICD10_HB_ALL LIKE '%O10%' 		
					OR idsn.MOM_DIAG_ICD10_PB_ALL LIKE '%O10%' THEN 1 ELSE 0 END ) AS dx_icd10_chtn,	
				MAX ( CASE WHEN idsn.MOM_DIAG_ICD10_HB_ALL LIKE '%O11%' 		
					OR idsn.MOM_DIAG_ICD10_PB_ALL LIKE '%O11%' THEN 1 ELSE 0 END ) AS dx_icd10_sipe,	
				MAX ( CASE WHEN idsn.MOM_DIAG_ICD10_HB_ALL LIKE '%O13%' 		
					OR idsn.MOM_DIAG_ICD10_PB_ALL LIKE '%O13%' THEN 1 ELSE 0 END ) AS dx_icd10_ghtn,	
				MAX ( CASE WHEN idsn.MOM_DIAG_ICD10_HB_ALL LIKE '%O15%' 		
					OR idsn.MOM_DIAG_ICD10_PB_ALL LIKE '%O15%' THEN 1 ELSE 0 END ) AS dx_icd10_ecl,	
				MAX (CASE WHEN upper(mom.ORDER_NAME) LIKE '%MAGNESIUM SULFATE%' 		
					AND upper(mom.ORDER_NAME) not LIKE '%IVPB%' 	
				-- specify infusion, vs. bolus for repletion		
					AND upper(mom.ORDER_FREQUENCY) = 'CONTINUOUS'	
					AND upper(mar.MAR_ACTION) IN ('NEW BAG', 'RATE VERIFY','CLINICIAN BOLUS (EPIDURAL ONLY)')	
					AND SECONDS_BETWEEN (ids.preg_del_date_time, mar.MAR_ADMINISTRATION_INSTANT) < 0	
					THEN 1 ELSE 0 END) AS ANY_AP_MAG_INFUSE,	
				MAX(CASE WHEN upper(mom.ORDER_NAME) LIKE '%MAGNESIUM SULFATE%' 		
					AND upper(mom.ORDER_NAME) not LIKE '%IVPB%' 	
					-- specify infusion, vs. bolus for repletion	
					AND upper(mom.ORDER_FREQUENCY) = 'CONTINUOUS'	
					AND upper(mar.MAR_ACTION) IN ('NEW BAG', 'RATE VERIFY', 'CLINICIAN BOLUS (EPIDURAL ONLY)')	
				-- require at least 60 minutes postpartum to exclude mg that was not turned off immediately pp		
					AND SECONDS_BETWEEN (ids.preg_del_date_time, mar.MAR_ADMINISTRATION_INSTANT) > 3600	
					THEN 1 ELSE 0 END) AS ANY_PP_MAG_INFUSE,	
				-- include max, min lab values for Magnesium to filter out folks who got Mg repletion		
				max (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (2778, 8321) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS lab_mg_max,		
				min (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (2778, 8321) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS lab_mg_min,		
				-- inlude max AST		
				max (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (7578, 2448) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS lab_ast_max,		
				-- include max ALT		
				max (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (7570,2443) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS lab_alt_max,		
				-- include PLATELETS		
				min (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (8808, 2951) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS lab_plt_min,		
				-- serum creatinine		
				max (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (2453, 7585) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS lab_cr_max,		
				-- 24 urine birth admission		
				max (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (2469) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS lab_prot24hur_max,		
				-- UPC birth admission		
				max (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (4212, 17885) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS lab_upc_max		
				FROM  "ASTUEBE".AMMI_PREG_01 ids		
				LEFT JOIN U223355."ids01_del_new" idsn ON (idsn.MOM_DEL_ENC = ids.MOM_PREG_DEL_ENC)		
				LEFT JOIN  _SYS_BIC."CDW.Reporting_View/MEDICATION_ORDER_MASTER" mom ON (mom.ENCOUNTER_KEY = ids.mom_preg_del_enc)		
				LEFT JOIN _SYS_BIC."CDW.Reporting_View/MEDICATION_ADMINISTRATION_MASTER" mar ON (mar.MEDICATION_ORDER_KEY=mom.MEDICATION_ORDER_KEY)		
				LEFT JOIN _SYS_BIC."CDW.Reporting_View/MEDICATION_MASTER" mm ON ( mm.MEDICATION_KEY= mom.ORDERABLE_MEDICATION_KEY )		
				LEFT JOIN _SYS_BIC."CDW.Reporting_View/LAB_COMPONENT_RESULT_MASTER" lcrm ON (ids.MOM_PREG_DEL_ENC = lcrm.ENCOUNTER_KEY  )		
				GROUP BY ids.MOM_PREG_DEL_ENC		
				)		
		SELECT DISTINCT ap.MOM_MRN , ap.PREG_DEL_DATE, ap.preg_id, ap.PREG_GA_WK ,				
		bpc.enc_max_sbp, bpc.enc_max_dbp, bpc.enc_maxbp_cat, bpc.enc_htn_cat,				
		be.dx_icd10_pre_sf,				
		be.dx_icd10_pre_nosf,				
		be.dx_icd10_chtn,				
		be.dx_icd10_ghtn,				
		be.dx_icd10_sipe,				
		be.dx_icd10_ecl,				
		be.ANY_AP_MAG_INFUSE,				
		be.ANY_PP_MAG_INFUSE,				
		be.lab_mg_max,				
		be.lab_mg_min,				
		be.lab_ast_max,				
		be.lab_alt_max,				
		be.lab_plt_min,				
		be.lab_cr_max,				
		be.lab_prot24hur_max,				
		be.lab_upc_max				
	 	FROM  birthenc be				
	 	LEFT JOIN "ASTUEBE".AMMI_PREG_01 ap ON (ap.MOM_PREG_DEL_ENC = be.MOM_PREG_DEL_ENC  )				
		LEFT JOIN "ASTUEBE".ammi_del_bpcat_01 bpc ON (bpc.encounter_key = be.MOM_PREG_DEL_ENC  )				
		)				
						
						
-- DATA PROFILING STARTS HERE						
-- validate that data behaves as expected						
			SELECT distinct idsn.DEL_HOSPITAL,			
			idsn.MOM_DIAG_ICD10_HB_ALL ,			
			ah.MOM_MRN,			
			ah.PREG_DEL_DATE,			
			ah.PREG_ID,			
			ah.PREG_GA_WK,			
			ah.ENC_MAX_SBP,			
			ah.ENC_MAX_DBP,			
			ah.ENC_MAXBP_CAT,			
			ah.ENC_HTN_CAT,			
			--ah.dx_icd10_pre_sf,			
			ah.DX_ICD10_PRE_NOSF,			
			--ah.DX_ICD10_CHTN,			
			--ah.DX_ICD10_SIPE,			
			--ah.DX_ICD10_GHTN,			
			--ah.DX_ICD10_ECL,			
			ah.ANY_AP_MAG_INFUSE,			
			ah.ANY_PP_MAG_INFUSE,			
			ah.LAB_MG_MAX,			
			ah.LAB_MG_MIN,			
			ah.LAB_AST_MAX,			
			ah.LAB_ALT_MAX,			
			ah.LAB_PLT_MIN,			
			ah.LAB_CR_MAX,			
			ah.LAB_PROT24HUR_MAX,			
			ah.LAB_UPC_MAX FROM ASTUEBE.ammi_HDP_01 ah			
			LEFT JOIN U223355."ids01_del_new" idsn ON (ah.preg_id = idsn.PREG_ID  )			
			WHERE ah.any_pp_mag_infuse = 1 AND ah.dx_icd10_pre_sf= 0 and  ah.dx_icd10_chtn= 0 and  ah.dx_icd10_sipe= 0 and  ah.dx_icd10_ghtn= 0 and  ah.dx_icd10_ecl=0			
			AND ah.PREG_DEL_DATE > '2016-01-01'			
			ORDER BY ah.dx_icd10_pre_nosf, ah.enc_htn_cat			
						
						
						
						
						
						
-- create data set w/ all magnesium administred - data profiling to inform "any mag pp" variable						
						
						
						
						
		SELECT DISTINCT 				
		ids.PREG_DEL_DATE, ids.mom_preg_del_enc , ids.PREG_ID ,  ids.preg_ga_wk,				
		-- ids.MOM_DIAG_ICD10_HB_ALL,				
		-- ids.MOM_DIAG_ICD10_PB_ALL,				
		mom.ORDER_NAME ,				
		mom.ORDERED_INSTANT ,				
		mar.MAR_ADMINISTRATION_INSTANT ,				
		CAST (SECONDS_BETWEEN (ids.preg_del_date_time, mar.MAR_ADMINISTRATION_INSTANT)/60 AS INT) AS mar_min_from_del,				
		CASE WHEN upper(mom.ORDER_NAME) LIKE '%MAGNESIUM SULFATE%' 				
			AND upper(mar.MAR_ACTION) IN ('NEW BAG', 'RATE VERIFY', 'CLINICIAN BOLUS (EPIDURAL ONLY)')			
			AND SECONDS_BETWEEN (ids.preg_del_date_time, mar.MAR_ADMINISTRATION_INSTANT) > 0			
			THEN 1 ELSE 0 END AS IS_PP_MAG_DOSE,			
		CASE WHEN upper(mom.ORDER_NAME) LIKE '%MAGNESIUM SULFATE%' 				
			AND upper(mar.MAR_ACTION) IN ('NEW BAG', 'RATE VERIFY','CLINICIAN BOLUS (EPIDURAL ONLY)')			
			AND SECONDS_BETWEEN (ids.preg_del_date_time, mar.MAR_ADMINISTRATION_INSTANT) < 0			
			THEN 1 ELSE 0 END AS IS_AP_MAG_DOSE,			
		mar.MAR_INFUSION_RATE ,				
		UPPER(mar.MAR_ACTION) ,				
		mar.MAR_DOSE				
		-- , mar.MEDICATION_ORDER_KEY,				
		-- mm.MEDICATION_NAME ,				
		-- mm.GENERIC_NAME				
		FROM "ASTUEBE".AMMI_PREG_01 ids				
		LEFT JOIN U223355."ids01_del_new" idsn ON (idsn.MOM_DEL_ENC = ids.MOM_PREG_DEL_ENC)				
		LEFT JOIN  _SYS_BIC."CDW.Reporting_View/MEDICATION_ORDER_MASTER" mom ON (mom.ENCOUNTER_KEY = ids.mom_preg_del_enc)				
		LEFT JOIN _SYS_BIC."CDW.Reporting_View/MEDICATION_ADMINISTRATION_MASTER" mar ON (mar.MEDICATION_ORDER_KEY=mom.MEDICATION_ORDER_KEY)				
		LEFT JOIN _SYS_BIC."CDW.Reporting_View/MEDICATION_MASTER" mm ON ( mm.MEDICATION_KEY= mom.ORDERABLE_MEDICATION_KEY )				
		WHERE  ids.PREG_DEL_DATE > '2023-01-01'				
		AND upper(mom.ORDER_NAME) LIKE '%MAGNESIUM SULFATE%'				
		ORDER BY ids.PREG_DEL_DATE, ids.MOM_PREG_DEL_ENC,  mom.ORDERED_INSTANT ,  mar.MAR_ADMINISTRATION_INSTANT 				
						
						
						
						
						
-- data profiling to find labs drawn at delivery hospitalization						
	SELECT DISTINCT lcm.LOINC_LONG_NAME  , lcm.BASE_NAME , lcm.COMMON_NAME , lcm.COMPONENT_NAME , lcm.LAB_COMPONENT_KEY 					
	FROM  "ASTUEBE".AMMI_PREG_01 ids					
	LEFT JOIN _SYS_BIC."CDW.Reporting_View/LAB_COMPONENT_RESULT_MASTER" lcrm ON (ids.MOM_PREG_DEL_ENC = lcrm.ENCOUNTER_KEY  )					
	LEFT JOIN  _SYS_BIC."CDW.Reporting_View/LAB_COMPONENT_MASTER" lcm ON (lcm.LAB_COMPONENT_KEY = lcrm.LAB_COMPONENT_KEY)					
	WHERE  ids.PREG_DEL_DATE_TIME > '2021-01-01' and					
	upper(lcm.COMMON_NAME) LIKE '%CREATININE%'					
						
	SELECT DISTINCT  lcm.COMPONENT_NAME , lcrm.LAB_COMPONENT_KEY , lcrm.NUMERIC_VALUE 					
	FROM  "ASTUEBE".AMMI_PREG_01 ids					
	LEFT JOIN _SYS_BIC."CDW.Reporting_View/LAB_COMPONENT_RESULT_MASTER" lcrm ON (ids.MOM_PREG_DEL_ENC = lcrm.ENCOUNTER_KEY  )					
	LEFT JOIN  _SYS_BIC."CDW.Reporting_View/LAB_COMPONENT_MASTER" lcm ON (lcm.LAB_COMPONENT_KEY = lcrm.LAB_COMPONENT_KEY)					
	WHERE  ids.PREG_DEL_DATE_TIME > '2021-01-01' and					
	lcrm.LAB_COMPONENT_KEY  IN (2778, 8321)					
						
	max (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (2778, 8321) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS mg_level_max,					
	min (CASE WHEN lcrm.LAB_COMPONENT_KEY  IN (2778, 8321) THEN lcrm.NUMERIC_VALUE ELSE NULL END) AS mg_level_min,					
						
						
						
						
						
						
			SELECT distinct idsn.DEL_HOSPITAL,			
			idsn.MOM_DIAG_ICD10_HB_ALL ,			
			ah.MOM_MRN,			
			ah.PREG_DEL_DATE,			
			ah.PREG_ID,			
			ah.PREG_GA_WK,			
			ah.ENC_MAX_SBP,			
			ah.ENC_MAX_DBP,			
			ah.ENC_MAXBP_CAT,			
			ah.ENC_HTN_CAT,			
			--ah.dx_icd10_pre_sf,			
			ah.DX_ICD10_PRE_NOSF,			
			--ah.DX_ICD10_CHTN,			
			--ah.DX_ICD10_SIPE,			
			--ah.DX_ICD10_GHTN,			
			--ah.DX_ICD10_ECL,			
			ah.ANY_AP_MAG_INFUSE,			
			ah.ANY_PP_MAG_INFUSE,			
			ah.LAB_MG_MAX,			
			ah.LAB_MG_MIN,			
			ah.LAB_AST_MAX,			
			ah.LAB_ALT_MAX,			
			ah.LAB_PLT_MIN,			
			ah.LAB_CR_MAX,			
			ah.LAB_PROT24HUR_MAX,			
			ah.LAB_UPC_MAX FROM ASTUEBE.ammi_HDP_01 ah			
			LEFT JOIN U223355."ids01_del_new" idsn ON (ah.preg_id = idsn.PREG_ID  )			
			WHERE ah.any_pp_mag_infuse = 1 AND ah.dx_icd10_pre_sf= 0 and  ah.dx_icd10_chtn= 0 and  ah.dx_icd10_sipe= 0 and  ah.dx_icd10_ghtn= 0 and  ah.dx_icd10_ecl=0			
			AND ah.PREG_DEL_DATE > '2016-01-01'			
			ORDER BY ah.dx_icd10_pre_nosf, ah.enc_htn_cat			
						
						
						
						
						
	-- individual data PROFILE					
	SELECT DISTINCT 					
	ids.PREG_DEL_DATE, ids.MOM_MRN  , ids.PREG_ID ,  ids.preg_ga_wk,					
	idsn.MOM_DIS_DATE_TIME ,					
	idsn.MOM_DIAG_ICD10_HB_ALL,					
	mom.ORDER_NAME , mar.MAR_DOSE, mom.DOSE_UNIT , mar.MAR_INFUSION_RATE , mar.MAR_INFUSION_RATE_UNIT ,					
	 mom.ORDER_FREQUENCY , 					
	UPPER(mar.MAR_ACTION) ,					
	mom.ORDERED_INSTANT ,					
	mar.MAR_ADMINISTRATION_INSTANT ,					
	CAST (SECONDS_BETWEEN (ids.preg_del_date_time, mar.MAR_ADMINISTRATION_INSTANT)/60 AS INT) AS mar_min_from_del,					
	CASE WHEN upper(mom.ORDER_NAME) LIKE '%MAGNESIUM SULFATE%' 					
		AND upper(mom.ORDER_NAME) not LIKE '%IVPB%' 				
	-- specify infusion, vs. bolus for repletion					
		AND upper(mom.ORDER_FREQUENCY) = 'CONTINUOUS'				
		AND upper(mar.MAR_ACTION) IN ('NEW BAG', 'RATE VERIFY', 'CLINICIAN BOLUS (EPIDURAL ONLY)')				
	-- require at least 60 minutes postpartum to exclude mg that was not turned off immediately pp					
		AND SECONDS_BETWEEN (ids.preg_del_date_time, mar.MAR_ADMINISTRATION_INSTANT) > 3600				
		THEN 1 ELSE 0 END AS IS_PP_MAG_INFUSE,				
	CASE WHEN upper(mom.ORDER_NAME) LIKE '%MAGNESIUM SULFATE%' 					
		AND upper(mom.ORDER_NAME) not LIKE '%IVPB%' 				
		AND upper(mar.MAR_ACTION) IN ('NEW BAG', 'RATE VERIFY','CLINICIAN BOLUS (EPIDURAL ONLY)')				
		AND SECONDS_BETWEEN (ids.preg_del_date_time, mar.MAR_ADMINISTRATION_INSTANT) < 0				
		THEN 1 ELSE 0 END AS IS_AP_MAG_INFUSE				
	FROM "ASTUEBE".AMMI_PREG_01 ids					
	LEFT JOIN U223355."ids01_del_new" idsn ON (idsn.MOM_DEL_ENC = ids.MOM_PREG_DEL_ENC)					
	LEFT JOIN  _SYS_BIC."CDW.Reporting_View/MEDICATION_ORDER_MASTER" mom ON (mom.ENCOUNTER_KEY = ids.mom_preg_del_enc)					
	LEFT JOIN _SYS_BIC."CDW.Reporting_View/MEDICATION_ADMINISTRATION_MASTER" mar ON (mar.MEDICATION_ORDER_KEY=mom.MEDICATION_ORDER_KEY)					
	LEFT JOIN _SYS_BIC."CDW.Reporting_View/MEDICATION_MASTER" mm ON ( mm.MEDICATION_KEY= mom.ORDERABLE_MEDICATION_KEY )					
	WHERE  ids.preg_id IN ( '75253808','59319531')					
	AND upper(mom.ORDER_NAME) LIKE '%MAGNESIUM SULFATE%'					
	ORDER BY ids.PREG_DEL_DATE, ids.MOM_MRN,  mom.ORDERED_INSTANT ,  mar.MAR_ADMINISTRATION_INSTANT 					
