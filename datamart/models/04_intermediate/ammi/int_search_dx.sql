-- depends_on: {{ ref('daterange') }}

{% set date_range_list = get_date_range('int_search_dx') %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

dx as (
    select
        patid,
        dx,
        dx_date
    from {{ ref('stg_pcornet__diagnosis') }}
),

dx_tmp as (
    select
        cohort.birthid,
        left(dx.dx, 5) as dx,
        min(dx.dx_date) as earliest_date
    from cohort
    left join dx on cohort.mother_patid = dx.patid
     and dx.dx_date between {{ date_range_list[0] }} and {{ date_range_list[1] }}
    group by cohort.birthid, left(dx.dx, 5)
),

renamed as (
    select
        *,
        count(birthid) over (partition by dx) as pat_counts
    from dx_tmp
)

select * from renamed where pat_counts >= 50
