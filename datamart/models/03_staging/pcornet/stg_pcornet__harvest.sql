
with source as (

    select * from {{ source('pcornet', 'harvest') }}

),

renamed as (

    select
        networkid,
        network_name,
        datamartid,
        datamart_name,
        datamart_platform,
        cdm_version,
        datamart_claims,
        datamart_ehr,
        birth_date_mgmt,
        enr_start_date_mgmt,
        enr_end_date_mgmt,
        admit_date_mgmt,
        discharge_date_mgmt,
        dx_date_mgmt,
        px_date_mgmt,
        rx_order_date_mgmt,
        rx_start_date_mgmt,
        rx_end_date_mgmt,
        dispense_date_mgmt,
        lab_order_date_mgmt,
        specimen_date_mgmt,
        result_date_mgmt,
        measure_date_mgmt,
        onset_date_mgmt,
        report_date_mgmt,
        resolve_date_mgmt,
        pro_date_mgmt,
        death_date_mgmt,
        medadmin_start_date_mgmt,
        medadmin_stop_date_mgmt,
        obsclin_start_date_mgmt,
        obsclin_stop_date_mgmt,
        obsgen_start_date_mgmt,
        obsgen_stop_date_mgmt,
        address_period_start_mgmt,
        address_period_end_mgmt,
        vx_record_date_mgmt,
        vx_admin_date_mgmt,
        vx_exp_date_mgmt,
        refresh_demographic_date,
        refresh_enrollment_date,
        refresh_encounter_date,
        refresh_diagnosis_date,
        refresh_procedures_date,
        refresh_vital_date,
        refresh_dispensing_date,
        refresh_lab_result_cm_date,
        refresh_condition_date,
        refresh_pro_cm_date,
        refresh_prescribing_date,
        refresh_pcornet_trial_date,
        refresh_death_date,
        refresh_death_cause_date,
        refresh_med_admin_date,
        refresh_obs_clin_date,
        refresh_provider_date,
        refresh_obs_gen_date,
        refresh_hash_token_date,
        refresh_lds_address_hx_date,
        refresh_immunization_date

    from source

)

select * from renamed

