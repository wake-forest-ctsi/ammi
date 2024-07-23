
class Queries():
    def update(self, timeframe):
      # put the cutoff time here
      self.demographic_base_sql_string = '''
      WITH gestage_tmp AS
      (
      SELECT 
        ENCOUNTERID,
        MIN(OBSCLIN_RESULT_NUM) AS "gest_age_in_days"
      FROM AMMI.dbo.OBS_CLIN
      WHERE OBSCLIN_TYPE = 'SM' AND OBSCLIN_CODE = '444135009' -- this is the code the gestational age
      GROUP BY ENCOUNTERID
      )
      ,
      -- get the delivery date from birthdate of baby
      delivery_date AS
      (
      SELECT
        a.BIRTHID,
        b.BIRTH_DATE AS 'delivery_date'  -- this already has time in it
      FROM AMMI.dbo.BIRTH_RELATIONSHIP a
      LEFT JOIN AMMI.dbo.DEMOGRAPHIC b ON a.PATID = b.PATID
      WHERE b.BIRTH_DATE IS NOT NULL  -- <10 records are removed 
      )
      ,
      demographic_base AS
      (
      SELECT
        a.BIRTHID,
        a.PATID,
        a.ENCOUNTERID,
        a.PREGNANCYID,
        a.MOTHERID,
        a.MOTHER_ENCOUNTERID,
        c.gest_age_in_days,
        DATEADD(DAY, -c.gest_age_in_days, b.delivery_date) AS "preg_start_date",
        b.delivery_date AS "preg_end_date",
        {} AS 'cutoff_time'  -- default to b.delivery_date
      FROM AMMI.dbo.BIRTH_RELATIONSHIP a
      INNER JOIN delivery_date b ON a.BIRTHID = b.BIRTHID
      INNER JOIN gestage_tmp c ON a.MOTHER_ENCOUNTERID = c.ENCOUNTERID
      )
      '''
      
      # update the timeframe
      self.demographic_base_sql_string = self.demographic_base_sql_string.format(timeframe)

      # the static parts:
      # delivery mode, complication, twins which are used to create the label
      # parity, previous cesarean, mum's height and mum's age at pregnancy
      # i think gest_age_in_days can be in here as it basically a known factor when mum starts labor
      # to make sure it can include all the information, add 3 days to the preg_end_date
      self.complication_sql_string = self.demographic_base_sql_string + ',' + '''
      /* failed induction of labor */
      /* NOTE: our data only contains ICD10 codes since 2019; please add ICD9 code if needed */
      failed_induction AS
      (
      SELECT
        *
      FROM AMMI.dbo.DIAGNOSIS
      WHERE DX LIKE 'O61%'
      )
      ,
      /* other complications */
      other_complications AS
      (
      SELECT
        *
      FROM AMMI.dbo.DIAGNOSIS
      WHERE DX LIKE 'O62%' OR DX LIKE 'O63%' OR DX LIKE 'O64%' OR DX LIKE 'O65%'
        OR DX LIKE 'O66%' OR DX LIKE 'O67%' OR DX LIKE 'O75.0%' OR DX LIKE 'O75.2%' OR DX LIKE 'O75.3%'
        OR DX LIKE 'O75.81%' OR DX LIKE 'O75.82%' OR DX LIKE 'O76%' OR DX LIKE 'O77%'
      )

      SELECT
        a.BIRTHID,
        MIN(a.preg_start_date) AS 'preg_start_date',
        MIN(a.gest_age_in_days) AS 'gest_age_in_days',
        SUM(CASE WHEN c.DX_DATE IS NULL THEN 0 ELSE 1 END) AS 'failed_labor',
        SUM(CASE WHEN d.DX_DATE IS NULL THEN 0 ELSE 1 END) AS 'other_complications',
        MIN(c.DX_DATE) AS 'earliest_failed_labor_date',
        MIN(d.DX_DATE) AS 'earliest_other_complication_date'
      FROM demographic_base a
      LEFT JOIN failed_induction c ON a.MOTHERID = c.PATID AND c.DX_DATE BETWEEN a.preg_start_date AND DATEADD(DAY, 3, a.preg_end_date)
      LEFT JOIN other_complications d ON a.MOTHERID = d.PATID AND d.DX_DATE BETWEEN a.preg_start_date AND DATEADD(DAY, 3, a.preg_end_date)
      GROUP BY a.BIRTHID
      ORDER BY a.BIRTHID;
      '''

      self.delivery_sql_string = '''
      /* check whether the cpt code for delivery is consistent with sm code
      if so, we can rely on the sm px date for starting time for labor
      see crack_delivery_code.xlsx online for the codes */
      WITH proc_tmp AS
      (
      SELECT
        a.*,
        b.PX,
        b.PX_DATE,
        b.PX_TYPE,
        b.RAW_PX,
        b.ENCOUNTERID AS 'b_encounterid'
      FROM AMMI.dbo.BIRTH_RELATIONSHIP a
      LEFT JOIN 
      (SELECT * FROM AMMI.dbo.[PROCEDURES] 
      WHERE PX IN (
      '59400','59409','59410', '59425','59426', -- vaginal
      '59510','59514','59515', -- cesarean
      '59610','59612','59614', -- vbac
      '59618','59620','59622'  -- cesarean after cesarean
      ) OR (PX LIKE '598%' AND PX NOT IN ('59870','59871','59897','59899')) -- abortion
      OR PX_TYPE = 'SM') b
      ON a.MOTHERID = b.PATID
      )
      ,
      cpt_delivery_mode AS
      (
      SELECT
        BIRTHID,
        MIN(b_ENCOUNTERID) AS 'MOTHER_ENCOUNTERID',
        SUM(CASE WHEN PX LIKE '594%' THEN 1 ELSE 0 END) AS 'vaginal',
        SUM(CASE WHEN PX LIKE '595%' THEN 1 ELSE 0 END) AS 'cesarean',
        SUM(CASE WHEN PX IN ('59610', '59612', '59614') THEN 1 ELSE 0 END) AS 'VBAC',
        SUM(CASE WHEN PX IN ('59618', '59620', '59622') THEN 1 ELSE 0 END) AS 'cesareanAC',
        SUM(CASE WHEN PX LIKE '598%' THEN 1 ELSE 0 END) AS 'abortion',
        MIN(PX_DATE) AS 'min_cpt_px_date',
        MAX(PX_DATE) AS 'max_cpt_px_date'
      FROM proc_tmp
      GROUP BY BIRTHID
      )
      ,
      sm_delivery_mode AS
      (
      SELECT
        BIRTHID,
        MIN(RAW_PX) AS 'min_sm_name',
        MAX(RAW_PX) AS 'max_sm_name',
        MIN(PX_DATE) AS 'min_sm_px_date',
        MAX(PX_DATE) AS 'max_sm_px_date'
      FROM proc_tmp
      WHERE PX_TYPE = 'SM'
      GROUP BY BIRTHID
      )
      ,
      delivery_mode AS
      (
      SELECT
        a.*,
        b.min_sm_name,
        b.max_sm_name,
        b.min_sm_px_date,
        b.max_sm_px_date
      FROM cpt_delivery_mode a
      LEFT JOIN sm_delivery_mode b ON a.BIRTHID = b.BIRTHID
      )

      SELECT 
        * 
      FROM delivery_mode
      ORDER BY BIRTHID
      '''

      self.twin_sql_string = '''
      SELECT
        a.BIRTHID,
        b.birth_counts
      FROM AMMI.dbo.BIRTH_RELATIONSHIP a
      LEFT JOIN 
      (SELECT MOTHER_ENCOUNTERID, COUNT(*) AS 'birth_counts'
      FROM AMMI.dbo.BIRTH_RELATIONSHIP
      GROUP BY MOTHER_ENCOUNTERID) b ON a.MOTHER_ENCOUNTERID = b.MOTHER_ENCOUNTERID
      ORDER BY BIRTHID;
      '''

      self.parity_sql_string = self.demographic_base_sql_string + '''
      SELECT
        a.BIRTHID,
        AVG(b.OBSCLIN_RESULT_NUM) AS 'parity'
      FROM demographic_base a
      LEFT JOIN (SELECT * FROM AMMI.dbo.OBS_CLIN WHERE OBSCLIN_CODE = '11977-6') b
      ON a.MOTHERID = b.PATID AND b.OBSCLIN_START_DATE BETWEEN a.preg_start_date AND DATEADD(DAY, 3, a.preg_end_date)
      GROUP BY a.BIRTHID
      ORDER BY a.BIRTHID
      '''

      self.previous_cesarean_sql_string = self.demographic_base_sql_string + ',' + '''
      previous_cesarean_tmp AS
      (
      SELECT
        PATID, DX, DX_DATE
      FROM AMMI.dbo.DIAGNOSIS
      WHERE DX LIKE 'O34.21%'
      )

      SELECT
        a.BIRTHID,
        MAX(CASE WHEN DX LIKE 'O34.21%' THEN 1 ELSE 0 END) AS 'previous_cesarean'
      FROM demographic_base a
      LEFT JOIN previous_cesarean_tmp b ON a.MOTHERID = b.PATID
      AND b.DX_DATE BETWEEN a.preg_start_date AND DATEADD(DAY, 3, a.preg_end_date)
      GROUP BY a.BIRTHID
      ORDER BY a.BIRTHID;
      '''

      self.other_parity_sql_string = self.demographic_base_sql_string + ',' + '''
      parity_tmp AS
      (
      SELECT
        PATID, DX, DX_DATE
      FROM AMMI.dbo.DIAGNOSIS
      WHERE DX LIKE 'Z34.0%' OR DX LIKE 'Z34.8%'
      )

      SELECT
        a.BIRTHID,
        MAX(CASE WHEN DX LIKE 'Z34.0%' THEN 1 ELSE 0 END) AS 'parity_1_recovered',
        MAX(CASE WHEN DX LIKE 'Z34.8%' THEN 1 ELSE 0 END) AS 'parity_2_recovered'
      FROM demographic_base a
      LEFT JOIN parity_tmp b ON a.MOTHERID = b.PATID
      AND b.DX_DATE BETWEEN a.preg_start_date AND DATEADD(DAY, 3, a.preg_end_date)
      GROUP BY a.BIRTHID
      ORDER BY a.BIRTHID;
      '''

      self.mum_age_sql_string = self.demographic_base_sql_string + '''
      SELECT
        a.BIRTHID,
        DATEDIFF(YEAR, b.BIRTH_DATE, a.preg_start_date) AS 'mum_age_at_preg_start_date'
      FROM demographic_base a
      LEFT JOIN AMMI.dbo.DEMOGRAPHIC b 
      ON a.MOTHERID = b.PATID
      ORDER BY a.BIRTHID
      '''

      self.mum_height_sql_string = self.demographic_base_sql_string + '''
      SELECT
        a.BIRTHID,
        MAX(HT) AS 'mum_height'
      FROM demographic_base a
      LEFT JOIN (SELECT PATID, HT, MEASURE_DATE FROM AMMI.dbo.VITAL WHERE HT IS NOT NULL) b 
      ON a.MOTHERID = b.PATID
      GROUP BY a.BIRTHID
      ORDER BY a.BIRTHID
      '''

      # bmi is always the first 12 weeks
      self.bmi_sql_string = self.demographic_base_sql_string + ',' + '''
      -- get bmi if exists during the first 12 weeks (84 days) of pregnancy
      -- well, if we only ask for minimal or average bmi, we can replace the window function with MIN
      bmi_stats AS
      (
      SELECT BIRTHID, earliest_bmi FROM 
      (SELECT
        a.BIRTHID,
        b.OBSCLIN_RESULT_NUM AS "earliest_bmi",
        ROW_NUMBER() OVER (PARTITION BY a.BIRTHID ORDER BY b.OBSCLIN_START_DATE) AS "k"  -- get the earliest bmi
      FROM demographic_base a 
      LEFT JOIN (SELECT * FROM AMMI.dbo.OBS_CLIN WHERE OBSCLIN_TYPE = 'LC' AND OBSCLIN_CODE = '39156-5') b -- code for BMI
        ON (a.MOTHERID = b.PATID) AND (b.OBSCLIN_START_DATE BETWEEN a.preg_start_date AND DATEADD(DAY, 84, a.preg_start_date))
      ) tmp
      WHERE k = 1
      )
      ,
      -- get the weight and height to recover null bmi
      weight_stats AS
      (
      SELECT BIRTHID, earliest_weight FROM 
      (SELECT
        a.BIRTHID,
        b.OBSCLIN_RESULT_NUM AS "earliest_weight",
        ROW_NUMBER() OVER (PARTITION BY a.BIRTHID ORDER BY b.OBSCLIN_START_DATE) AS "k"  -- get the earliest weight
      FROM demographic_base a 
      LEFT JOIN (SELECT * FROM AMMI.dbo.OBS_CLIN WHERE OBSCLIN_TYPE = 'LC' AND OBSCLIN_CODE = '3141-9') b -- for weight
        ON (a.MOTHERID = b.PATID) AND (b.OBSCLIN_START_DATE BETWEEN a.preg_start_date AND DATEADD(DAY, 84, a.preg_start_date))
      ) tmp 
      WHERE k = 1
      )
      ,
      -- get the max height
      height_stats AS
      (
      SELECT
        a.BIRTHID,
        MAX(b.OBSCLIN_RESULT_NUM) AS "max_height"
      FROM demographic_base a
      LEFT JOIN (SELECT * FROM AMMI.dbo.OBS_CLIN WHERE OBSCLIN_TYPE = 'LC' AND OBSCLIN_CODE = '3137-7') b -- for height
        ON a.MOTHERID = b.PATID
      GROUP BY a.BIRTHID
      )

      -- now join every one with the demographic_base
      SELECT 
        a.*,
        b.earliest_weight,
        c.max_height,
        d.earliest_bmi,
        b.earliest_weight / SQUARE(c.max_height) * 703 AS "computed_bmi"
      FROM demographic_base a
      LEFT JOIN weight_stats b ON a.BIRTHID = b.BIRTHID
      LEFT JOIN height_stats c ON a.BIRTHID = c.BIRTHID
      LEFT JOIN bmi_stats d ON a.BIRTHID = d.BIRTHID
      ORDER BY a.BIRTHID;
      '''

      # diabetes, counts, preeclampsia, bloodpressure and other obesity needs to track the end time
      self.diabetes_sql_string = self.demographic_base_sql_string + ',' + '''
      -- patient with diabetes diagnosis code
      -- need to ask what's the difference between E and O code, does E during pregnancy count?
      diabetes_diag AS
      (
      SELECT PATID, DX, DX_DATE
      FROM AMMI.dbo.DIAGNOSIS
      WHERE DX LIKE 'E08%' OR DX LIKE 'E09%' OR DX LIKE 'E10%' OR DX LIKE 'E11%' OR DX LIKE 'E13%' OR DX LIKE 'O24%'
      )

      SELECT
        a.BIRTHID,
        MAX(CASE WHEN c.DX_DATE IS NULL THEN 0 ELSE 1 END) AS 'gestational_diabetes',
        MIN(c.DX_DATE) AS 'earliest_gestational_diabetes_diag_date'
      FROM demographic_base a
      LEFT JOIN diabetes_diag c ON a.MOTHERID = c.PATID AND c.DX_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      GROUP BY a.BIRTHID
      ORDER BY a.BIRTHID;
      '''

      self.counts_sql_string = self.demographic_base_sql_string + ',' + '''
      encounter_counts AS
      (
      SELECT 
        a.BIRTHID,
        SUM(CASE WHEN b.ENCOUNTERID IS NULL THEN 0 ELSE 1 END) AS 'encounter_counts'
      FROM demographic_base a
      LEFT JOIN AMMI.dbo.ENCOUNTER b ON a.MOTHERID = b.PATID
      AND b.ADMIT_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      GROUP BY a.BIRTHID
      )
      ,
      encounter_earliest AS
      (
      SELECT
        a.BIRTHID,
        MIN(b.ADMIT_DATE) AS 'earliest_encounter_day'
      FROM demographic_base a
      LEFT JOIN AMMI.dbo.ENCOUNTER b ON a.MOTHERID = b.PATID 
      AND b.ADMIT_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      GROUP BY a.BIRTHID
      )
      ,
      diagnosis_counts AS
      (
      SELECT 
        a.BIRTHID,
        COUNT(DISTINCT DX) AS 'unique_diag_counts'
      FROM demographic_base a
      LEFT JOIN AMMI.dbo.DIAGNOSIS b ON a.MOTHERID = b.PATID 
      AND b.DX_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      GROUP BY a.BIRTHID
      )

      SELECT
        a.BIRTHID,
        a.encounter_counts,
        b.earliest_encounter_day,
        c.unique_diag_counts
      FROM encounter_counts a
      LEFT JOIN encounter_earliest b ON a.BIRTHID = b.BIRTHID
      LEFT JOIN diagnosis_counts c ON a.BIRTHID = c.BIRTHID
      ORDER BY a.BIRTHID;
      '''

      self.preeclampsia_sql_string = self.demographic_base_sql_string + ',' + '''
      preeclampsia_diagnosis AS
      (
        select 
          a.BIRTHID,
          min(a.preg_start_date) as 'preg_start_date',
          min(b.dx_date) as 'earliest_diagnosis_date',
          max(b.dx_date) as 'latest_diagnosis_date'
        from 
          demographic_base a
          left join 
            (select dx_type, dx, dx_date, patid 
            from AMMI.dbo.DIAGNOSIS
            where dx_type = '10' and dx like 'O14%') b
          on a.motherid = b.patid
              and b.dx_date between a.preg_start_date AND a.cutoff_time
        group by a.BIRTHID
      )
      ,
      /* Birth episodes with preeclampsia supporting laboratory measurements */
      lab_tmp as
      (
        select 
          *
        from AMMI.dbo.LAB_RESULT_CM a
        where 
          (a.lab_loinc in ('2889-4') and a.result_num >= 300 and a.result_modifier in ('EQ', 'GE', 'GT'))		-- Proteinuria
        or (a.lab_loinc in ('2890-2') and a.result_num >= 0.3 and a.result_modifier in ('EQ', 'GE', 'GT'))	-- Proteinuria
        -- Proteinuria dipstick ??
        or (a.lab_loinc in ('777-3') and a.result_num <= 99999 and a.result_modifier in ('EQ', 'LE', 'LT'))	-- Thrombocytopenia
        or (a.lab_loinc in ('2160-0') and a.result_num >= 1.2 and a.result_modifier in ('EQ', 'GE', 'GT'))	-- Renal Insufficiency
      )
      ,
      lab_diagnosis as
      (
        select
          a.BIRTHID,
          min(b.lab_order_date) as 'earliest_lab_confirm_date',
          max(b.lab_order_date) as 'latest_lab_confirm_date'
        from demographic_base a
        left join lab_tmp b on a.motherid = b.patid and b.lab_order_date between a.preg_start_date and a.cutoff_time
        group by a.birthid
      )
      SELECT
        a.BIRTHID,
        a.earliest_diagnosis_date,
        a.latest_diagnosis_date,
        b.earliest_lab_confirm_date,
        b.latest_lab_confirm_date
      FROM preeclampsia_diagnosis a
      LEFT JOIN lab_diagnosis b ON a.BIRTHID = b.BIRTHID
      ORDER BY a.BIRTHID;
      '''

      self.bloodpressure_sql_string = self.demographic_base_sql_string + ',' + '''
      vitals AS
      (
      SELECT
        a.*,
        b.SYSTOLIC,
        b.DIASTOLIC,
        b.MEASURE_DATE + b.MEASURE_TIME AS 'measure_time'
      FROM demographic_base a
      LEFT JOIN (SELECT * FROM AMMI.dbo.VITAL WHERE SYSTOLIC IS NOT NULL AND DIASTOLIC IS NOT NULL) b
      ON a.MOTHERID = b.PATID AND (b.MEASURE_DATE + b.MEASURE_TIME) BETWEEN DATEADD(WEEK, 20, a.preg_start_date) AND a.cutoff_time
      )
      ,
      avg_vitals AS
      (
        SELECT BIRTHID, AVG(SYSTOLIC) AS 'avg_systolic', AVG(DIASTOLIC) AS 'avg_diastolic', 
              SUM(CASE WHEN SYSTOLIC IS NOT NULL AND DIASTOLIC IS NOT NULL THEN 1 ELSE 0 END) AS 'total_counts' -- simple count will return 1 for null
        FROM vitals
        GROUP BY BIRTHID
      )
      ,
      high_systolic AS
      (
      SELECT
        BIRTHID,
        MIN(measure_time) AS 'earliest_time_with_high_systolic',
        MAX(measure_time) AS 'latest_time_with_high_systolic',
        COUNT(*) AS 'count_with_high_systolic'
      FROM vitals WHERE SYSTOLIC >= 140
      GROUP BY BIRTHID
      )
      ,
      severe_high_systolic AS
      (
      SELECT
        BIRTHID,
        MIN(measure_time) AS 'earliest_time_with_severe_high_systolic',
        MAX(measure_time) AS 'latest_time_with_severe_high_systolic',
        COUNT(*) AS 'count_with_severe_high_systolic'
      FROM vitals WHERE SYSTOLIC >= 160
      GROUP BY BIRTHID
      )
      ,
      max2_systolic_tmp AS
      (
      SELECT
        a.BIRTHID,
        a.SYSTOLIC AS 'highest_systolic',
        b.SYSTOLIC AS 'second_highest_systolic',
        a.SYSTOLIC + b.SYSTOLIC AS 'avg_tmp'
      FROM vitals a
      LEFT JOIN vitals b ON a.BIRTHID = b.BIRTHID AND DATEDIFF(HOUR, a.measure_time, b.measure_time) > 4  -- 4 hours apart
      )
      ,
      max2_systolic AS
      (
      SELECT 
        a.BIRTHID,
        a.highest_systolic,
        a.second_highest_systolic
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY BIRTHID ORDER BY avg_tmp DESC) AS 'k' FROM max2_systolic_tmp) a
      WHERE a.k = 1
      )
      ,
      high_diastolic AS
      (
      SELECT
        BIRTHID,
        MIN(measure_time) AS 'earliest_time_with_high_diastolic',
        MAX(measure_time) AS 'latest_time_with_high_diastolic',
        COUNT(*) AS 'count_with_high_diastolic'
      FROM vitals WHERE DIASTOLIC >= 90
      GROUP BY BIRTHID
      )
      ,
      severe_high_diastolic AS
      (
      SELECT
        BIRTHID,
        MIN(measure_time) AS 'earliest_time_with_severe_high_diastolic',
        MAX(measure_time) AS 'latest_time_with_severe_high_diastolic',
        COUNT(*) AS 'count_with_severe_high_diastolic'
      FROM vitals WHERE DIASTOLIC >= 110
      GROUP BY BIRTHID
      )
      ,
      max2_diastolic_tmp AS
      (
      SELECT
        a.BIRTHID,
        a.DIASTOLIC AS 'highest_diastolic',
        b.DIASTOLIC AS 'second_highest_diastolic',
        a.DIASTOLIC + b.DIASTOLIC AS 'avg_tmp'
      FROM vitals a
      LEFT JOIN vitals b ON a.BIRTHID = b.BIRTHID AND DATEDIFF(HOUR, a.measure_time, b.measure_time) > 4  -- 4 hours apart
      )
      ,
      max2_diastolic AS
      (
      SELECT 
        a.BIRTHID,
        a.highest_diastolic,
        a.second_highest_diastolic
      FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY BIRTHID ORDER BY avg_tmp DESC) AS 'k' FROM max2_diastolic_tmp) a
      WHERE a.k = 1
      )

      SELECT
        a.BIRTHID,
        b.count_with_high_systolic,
        b.earliest_time_with_high_systolic,
        b.latest_time_with_high_systolic,
        c.count_with_severe_high_systolic,
        c.earliest_time_with_severe_high_systolic,
        c.latest_time_with_severe_high_systolic,
        d.count_with_high_diastolic,
        d.earliest_time_with_high_diastolic,
        d.latest_time_with_high_diastolic,
        e.count_with_severe_high_diastolic,
        e.earliest_time_with_severe_high_diastolic,
        e.latest_time_with_severe_high_diastolic,
        f.avg_systolic,
        f.avg_diastolic,
        f.total_counts,
        g.highest_systolic,
        g.second_highest_systolic,
        h.highest_diastolic,
        h.second_highest_diastolic
      FROM demographic_base a
      LEFT JOIN high_systolic b ON a.BIRTHID = b.BIRTHID
      LEFT JOIN severe_high_systolic c ON a.BIRTHID = c.BIRTHID
      LEFT JOIN high_diastolic d ON a.BIRTHID = d.BIRTHID
      LEFT JOIN severe_high_diastolic e ON a.BIRTHID = e.BIRTHID
      LEFT JOIN avg_vitals f ON a.BIRTHID = f.BIRTHID
      LEFT JOIN max2_systolic g ON a.BIRTHID = g.BIRTHID
      LEFT JOIN max2_diastolic h ON a.BIRTHID = h.BIRTHID
      ORDER BY BIRTHID;
      '''

      self.other_obese_sql_string = self.demographic_base_sql_string + ',' + '''
      obese_tmp AS
      (
      SELECT
        PATID, DX, DX_DATE
      FROM AMMI.dbo.DIAGNOSIS
      WHERE DX LIKE 'E66%' OR DX LIKE 'O99.21%'
      )

      SELECT
        a.BIRTHID,
        MIN(CASE WHEN DX = 'E66.01' THEN DATEDIFF(DAY, a.preg_start_date, DX_DATE) ELSE 300 END) AS 'morbid',
        MIN(CASE WHEN DX IN ('E66.09', 'E66.8', 'E66.9') THEN DATEDIFF(DAY, a.preg_start_date, DX_DATE) ELSE 300 END) AS 'obese',
        MIN(CASE WHEN DX = 'E66.3' THEN DATEDIFF(DAY, a.preg_start_date, DX_DATE) ELSE 300 END) AS 'overweight',
        MIN(CASE WHEN DX LIKE 'O99%' THEN DATEDIFF(DAY, a.preg_start_date, DX_DATE) ELSE 300 END) AS 'O99_obese'
      FROM demographic_base a
      LEFT JOIN obese_tmp b ON a.MOTHERID = b.PATID
      AND b.DX_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      GROUP BY a.BIRTHID
      ORDER BY a.BIRTHID;;
      '''

      # get the prescribing table
      self.get_rx_sql_string = self.demographic_base_sql_string + ',' + '''
      rx_list AS
      (
      SELECT
        RXNORM_CUI,
        COUNT(DISTINCT a.BIRTHID) AS 'pat_counts'
      FROM demographic_base a
      INNER JOIN AMMI.dbo.PRESCRIBING b on a.MOTHERID = b.PATID
        AND b.RX_ORDER_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      WHERE RXNORM_CUI IS NOT NULL
      GROUP BY RXNORM_CUI
      )
      ,
      rx_tmp AS
      (
      SELECT
        a.BIRTHID,
        -- b.RX_ORDER_DATE,
        SUBSTRING(b.RAW_RX_MED_NAME, 1, 20) AS 'med_name',
        CASE WHEN RX_QUANTITY IS NULL THEN 1 ELSE RX_QUANTITY END AS 'rx_quantity_norm',
        CASE WHEN RX_REFILLS IS NULL THEN 0 ELSE RX_REFILLS END AS 'rx_refills_norm'
      FROM demographic_base a
      INNER JOIN 
      (SELECT * FROM AMMI.dbo.PRESCRIBING WHERE RXNORM_CUI IN (SELECT RXNORM_CUI FROM rx_list WHERE pat_counts > 100)) b 
      ON a.MOTHERID = b.PATID AND b.RX_ORDER_DATE BETWEEN a.preg_start_date and a.cutoff_time
      )

      SELECT
        DISTINCT BIRTHID, med_name,
        SUM(rx_quantity_norm * (rx_refills_norm + 1)) OVER (PARTITION BY BIRTHID, med_name) AS 'total_quantity'
      FROM rx_tmp
      ORDER BY BIRTHID;
      '''

      self.get_dx_sql_string = self.demographic_base_sql_string + ',' + '''
      dx_list AS
      (
      SELECT
        DX,
        COUNT(DISTINCT a.BIRTHID) AS 'pat_counts'
      FROM demographic_base a
      INNER JOIN AMMI.dbo.DIAGNOSIS b on a.MOTHERID = b.PATID
        AND b.DX_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      WHERE DX IS NOT NULL
      GROUP BY DX
      )
      ,
      dx_tmp AS
      (
      SELECT
        a.BIRTHID,
        b.DX,
        b.DX_DATE
      FROM demographic_base a
      INNER JOIN 
        (SELECT * FROM AMMI.dbo.DIAGNOSIS WHERE DX IN (SELECT DX FROM dx_list WHERE pat_counts > 100)) b on a.MOTHERID = b.PATID
        AND b.DX_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      )
      ,
      dx_rank AS
      (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY BIRTHID, DX ORDER BY DX_DATE ASC) AS 'earliest_rank',
        ROW_NUMBER() OVER (PARTITION BY BIRTHID, DX ORDER BY DX_DATE DESC) AS 'latest_rank'
      FROM dx_tmp
      )

      SELECT * FROM dx_rank WHERE earliest_rank = 1 OR latest_rank = 1;
      '''

      self.get_px_sql_string = self.demographic_base_sql_string + ',' + '''
      px_list AS
      (
      SELECT
        PX,
        COUNT(DISTINCT a.BIRTHID) AS 'pat_counts'
      FROM demographic_base a
      INNER JOIN AMMI.dbo.[PROCEDURES] b on a.MOTHERID = b.PATID
        AND b.PX_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      WHERE PX IS NOT NULL
      GROUP BY PX
      )
      ,
      px_tmp AS
      (
      SELECT
        a.BIRTHID,
        b.PX,
        b.PX_DATE
      FROM demographic_base a
      INNER JOIN 
        (SELECT * FROM AMMI.dbo.[PROCEDURES] WHERE PX IN (SELECT PX FROM px_list WHERE pat_counts > 100)) b on a.MOTHERID = b.PATID
        AND b.PX_DATE BETWEEN a.preg_start_date AND a.cutoff_time
      )
      ,
      px_rank AS
      (
      SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY BIRTHID, PX ORDER BY PX_DATE ASC) AS 'earliest_rank',
        ROW_NUMBER() OVER (PARTITION BY BIRTHID, PX ORDER BY PX_DATE DESC) AS 'latest_rank'
      FROM px_tmp
      )

      SELECT
        BIRTHID,
        PX,
        PX_DATE
      FROM px_rank WHERE earliest_rank = 1;
      '''






      self.useful_lab_sql_string = self.demographic_base_sql_string + ',' + '''
lab_useful AS
(
SELECT * FROM AMMI.dbo.LAB_RESULT_CM
WHERE RAW_LAB_NAME IN ('Leukocytes [#/volume] in Blood by Manual count',
'Glucose [Mass/volume] in Serum or Plasma',
'Platelets [#/volume] in Blood by Automated count')
)
,
lab_tmp AS
(
SELECT
  a.BIRTHID,
  b.RAW_LAB_NAME,
  b.RESULT_NUM,
  b.SPECIMEN_DATE
FROM demographic_base a
INNER JOIN lab_useful b ON a.MOTHERID = b.PATID
  AND b.SPECIMEN_DATE BETWEEN a.preg_start_date AND a.cutoff_time
  AND RESULT_NUM IS NOT NULL
)

SELECT
  DISTINCT BIRTHID, RAW_LAB_NAME,
  AVG(RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_LAB_NAME) AS 'avg_value',
  MIN(RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_LAB_NAME) AS 'min_value',
  MAX(RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_LAB_NAME) AS 'max_value',
  COUNT(*) OVER (PARTITION BY BIRTHID, RAW_LAB_NAME) AS 'counts',
  STDEV(RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_LAB_NAME) AS 'std',
  PERCENTILE_DISC(0.25) WITHIN GROUP (ORDER BY RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_LAB_NAME) AS '25_value',
  PERCENTILE_DISC(0.75) WITHIN GROUP (ORDER BY RESULT_NUM) OVER (PARTITION BY BIRTHID, RAW_LAB_NAME) AS '75_value'
FROM lab_tmp;
'''