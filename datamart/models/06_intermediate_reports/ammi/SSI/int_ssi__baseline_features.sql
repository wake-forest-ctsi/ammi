-- get all the features that do not depend on study periods

{{ config(materialized='table') }}

with all_birthids as (
    select
        birthid,
        'delivery_year' as 'feature_name',
        datepart(year, baby_birth_date) as 'value'
    from {{ ref('int_ssi__cohort') }}

    union all

    select
        birthid,
        'mother_age_at_birth' as 'feature_name',
        mother_age as 'value'
    from {{ ref('int_mother_age_at_birth') }}

    union all

    select
        birthid,
        'mother_height' as 'feature_name',
        mother_height as 'value'
    from {{ ref('int_mother_height') }}

    union all

    select
        birthid,
        'mother_is_hispanic' as 'feature_name',
        mother_is_hispanic as 'value'
    from {{ ref('int_race') }}

    union all

    select
        birthid,
        'mother_is_white' as 'feature_name',
        mother_is_white as 'value'
    from {{ ref('int_race') }}

    union all

    select
        birthid,
        'mother_is_black' as 'feature_name',
        mother_is_black as 'value'
    from {{ ref('int_race') }}

    union all

    select
        birthid,
        'gest_age_in_days' as 'feature_name',
        gest_age_in_days as 'value'
    from {{ ref('int_gestational_age') }}

    union all

    select
        birthid,
        'gest_age_is_null' as 'feature_name',
        gest_age_is_null as 'value'
    from {{ ref('int_gestational_age') }}

    union all

    select
        birthid,
        'parity' as 'feature_name',
        parity as 'value'
    from {{ ref('int_parity') }}

    union all

    select
        birthid,
        'parity_1_recovered' as 'feature_name',
        parity_1_recovered as 'value'
    from {{ ref('int_other_parity') }}

    union all

    select
        birthid,
        'parity_2_recovered' as 'feature_name',
        parity_2_recovered as 'value'
    from {{ ref('int_other_parity') }}

    union all

    select
        birthid,
        delivery_mode as 'feature_name',
        1 as 'value'
    from {{ ref('int_delivery_mode_grouped') }}
)

select
    all_birthids.*
from all_birthids
inner join {{ ref('int_ssi__cohort')}} ssi_cohort on all_birthids.birthid = ssi_cohort.birthid