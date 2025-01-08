-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_insurance') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

visits as (
    select
        birthid,
        admit_date,
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
     and visits.admit_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
    group by cohort.birthid
)

select * from renamed

