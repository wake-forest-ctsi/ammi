
with source as (

    select * from {{ source('pcornet', 'pcornet_trial') }}

),

renamed as (

    select
        patid,
        trialid,
        participantid,
        trial_siteid,
        trial_enroll_date,
        trial_end_date,
        trial_withdraw_date,
        trial_invite_code

    from source

)

select * from renamed

