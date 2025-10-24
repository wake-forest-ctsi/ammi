{% macro all_insurance_features_macro(cohort_table, date1, date2) %}

-- get the payer_type_primary for now; remove it is 'NI'

with cohort as (
    select
        *
    from {{ cohort_table }}
),

visits as (
    select
        birthid,
        admit_date,  -- int_visits has fixed the date
        'insurance_' + payer_type_primary as 'insurance'
    from {{ ref('int_visits') }}
    where payer_type_primary != 'NI'
),

renamed as (
    select
        cohort.birthid,
        visits.insurance
    from cohort
    inner join visits on cohort.birthid = visits.birthid
     and visits.admit_date between {{ date1 }} and {{ date2 }}
)

select * from renamed

{% endmacro %}