{% macro insurance_macro(date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

visits as (
    select
        birthid,
        admit_date,  -- int_visits has fixed the date
        payer_type_primary
    from {{ ref('int_visits') }}
),

renamed as (
    select
        cohort.birthid,
        {{ dbt_utils.pivot('payer_type_primary',
                           dbt_utils.get_column_values(ref('int_visits'), 'payer_type_primary', where="payer_type_primary != 'NI'"),
                           agg='max',
                           then_value=1,
                           else_value=0,
                           prefix='insurance_')}}
    from cohort 
    left join visits on cohort.birthid = visits.birthid
     and visits.admit_date between {{ date1 }} and {{ date2 }}
    group by cohort.birthid
)

select * from renamed

{% endmacro %}