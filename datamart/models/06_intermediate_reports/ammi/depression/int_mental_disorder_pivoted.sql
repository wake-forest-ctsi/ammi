-- depends_on: {{ ref('int_mental_disorder') }}

with tmp as (
    select
        birthid,
        dx
    from int_mental_disorder
),

-- may need to think about using max instead of sum
renamed as (
    select
        birthid,
        {{ dbt_utils.pivot('dx',
                           dbt_utils.get_column_values(ref('int_mental_disorder'), 'dx'),
                           agg='sum', 
                           then_value=1,
                           else_value=0,
                           suffix='_diagnosis') }}
    from tmp
    group by birthid
)

select * from renamed