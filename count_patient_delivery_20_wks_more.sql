with pop as (
    select distinct
        DeliveryRecordHsb.SUMMARY_BLOCK_ID,
        PregnancyEpisode.PAT_LINK_ID AS MOM_PAT_ID,
        PATIENT.BIRTH_DATE AS DELIVERY_INSTANT,
        DeliveryRecordHsb.OB_DEL_BIRTH_DTTM as BABY_BIRTH_DATE,
     
        PATIENT_3.PED_GEST_AGE_DAYS as gest_age_in_days,
        PATIENT_3.PED_GEST_AGE_NUM as gest_age_in_weeks,
        ZC_DELIVERY_TYPE.NAME AS DELIVERY_METHOD,      
        pat.pat_mrn_id as MOM_MRN_ID,
        pat.birth_date as  MOM_DOB    
    
    FROM OB_HSB_DELIVERY@clarityprod DeliveryRecordHsb
     INNER JOIN ( SELECT MomDeliveryEpisode.EPISODE_ID, MomDeliveryEpisode.START_DATE,  MomDeliveryEpisode.END_DATE,
                            MomDeliveryEpisode.OB_DEL_PREG_EPI_ID, 
                            COALESCE( MomDeliveryEpisode.OB_DELIVERY_BABY_ID, BabyDeliveryEpisode.OB_DELIVERY_BABY_ID ) as OB_DELIVERY_BABY_ID
                       FROM EPISODE@clarityprod MomDeliveryEpisode
                         LEFT OUTER JOIN EPISODE@clarityprod BabyDeliveryEpisode 
                           ON MomDeliveryEpisode.EPISODE_ID = BabyDeliveryEpisode.OB_DEL_REC_COPY_ID
                       WHERE MomDeliveryEpisode.OB_DEL_PREG_EPI_ID IS NOT NULL
                         AND ( MomDeliveryEpisode.OB_DELIVERY_BABY_ID IS NOT NULL OR BabyDeliveryEpisode.OB_DELIVERY_BABY_ID IS NOT NULL )
                         AND NULLIF( MomDeliveryEpisode.STATUS_C, 3 ) IS NOT NULL 
                         ) DeliveryEpisode
        ON DeliveryRecordHsb.SUMMARY_BLOCK_ID = DeliveryEpisode.EPISODE_ID
    
        INNER JOIN EPISODE@clarityprod PregnancyEpisode
          ON DeliveryEpisode.OB_DEL_PREG_EPI_ID = PregnancyEpisode.EPISODE_ID
          
        --mom demo data
        join patient@clarityprod pat on pat.pat_id = PregnancyEpisode.PAT_LINK_ID  
    
        
       LEFT OUTER JOIN EPISODE_2@clarityprod
          ON PregnancyEpisode.EPISODE_ID = EPISODE_2.EPISODE_ID
       
       LEFT OUTER JOIN OB_HSB_DELIVERY_2@clarityprod
          ON DeliveryRecordHsb.SUMMARY_BLOCK_ID = OB_HSB_DELIVERY_2.SUMMARY_BLOCK_ID
     
       LEFT OUTER JOIN PATIENT@clarityprod
          ON DeliveryEpisode.OB_DELIVERY_BABY_ID = PATIENT.PAT_ID
       
        LEFT OUTER JOIN PATIENT_3@clarityprod
          ON DeliveryEpisode.OB_DELIVERY_BABY_ID = PATIENT_3.PAT_ID
       
       LEFT OUTER JOIN  ZC_DELIVERY_TYPE@clarityprod 
          ON DeliveryRecordHsb.OB_DEL_DELIV_METH_C = ZC_DELIVERY_TYPE.DELIVERY_TYPE_C     
      
      where DeliveryRecordHsb.OB_DELIVERY_DATE between  '1-JAN-2019' and  '31-DEC-2021'
      and DeliveryRecordHsb.OB_DEL_EPIS_TYPE_C = 10 --Obstetrics - Delivery
    
)

/* Total Number of deliveries*/
 select count(*) as TOT_CNT from pop
 where gest_age_in_weeks > 20
 and floor(( baby_birth_date - mom_dob)/365.25) between 15 and 54
;

--## uncoment SQL and run to get results
--/*Count deliveries group by gestational age*/
--select gest_age_in_weeks, count(*) as CNT
--from pop
--where gest_age_in_weeks > 20
--and floor(( baby_birth_date - mom_dob)/365.25) between 15 and 54
--group by gest_age_in_weeks
--order by gest_age_in_weeks
--;

--/*Count deliveries group by delivery_method */
--select  delivery_method, count(*) as CNT from pop
--where gest_age_in_weeks > 20
--and floor(( baby_birth_date - mom_dob)/365.25) between 15 and 54
--group by   delivery_method
--order by delivery_method
--;

--/*Count deliveries group by delivery_method (summary) */
--select 
--sum(case when delivery_method like 'C-Section%' then 1 else 0 end) as C_Section_CNT
--,sum(case when delivery_method like 'Vaginal%' then 1 else 0 end) as Vaginal_CNT
--,sum(case when delivery_method like 'Induction%' then 1 else 0 end) as Induction_CNT
--,sum(case when delivery_method like 'VBAC%' then 1 else 0 end) as VBAC_CNT
--
--from pop
--where gest_age_in_weeks > 20
--and floor(( baby_birth_date - mom_dob)/365.25) between 15 and 54
--;


