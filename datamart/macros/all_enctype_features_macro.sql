{% macro all_enctype_features_macro(cohort_table, date1, date2) %}

-- currently doing all the counts for enc_type
-- remov enc_type = 'NI' OR 'UN'
-- use a distinct to remove duplicated entry for the same visit

with cohort as (
    select
        *
    from {{ cohort_table }}
),

visits as (
    select
        distinct     -- use disinct here
        birthid,
        admit_date,  -- this is only good to date so to remove duplicated entry
        'enc_type_' + enc_type as enc_type
    from {{ ref('int_visits') }}
    where enc_type != 'NI' and enc_type != 'UN'
),

renamed as (
    select
        cohort.birthid,
        visits.enc_type
    from cohort
    inner join visits on cohort.birthid = visits.birthid
     and visits.admit_date between {{ date1 }} and {{ date2 }}
)

select * from renamed

{% endmacro %}