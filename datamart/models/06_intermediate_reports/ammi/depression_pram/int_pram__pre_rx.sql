with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

rx as (
    select
        patid,
        {{ add_time_to_date_macro("rx_order_date", "rx_order_time") }} rx_order_date
    from {{ ref('prescribing') }}
),

encounter as (
    select
        patid,
        {{ add_time_to_date_macro("admit_date", "admit_time") }} admit_date
    from {{ ref('encounter') }}
),

renamed as (
    select
        a.birthid,
        count(distinct b.rx_order_date) as order_counts,
        count(distinct c.admit_date) as admit_counts
    from cohort a
    left join rx b on a.mother_patid = b.patid
     and b.rx_order_date between dateadd(year, -1, a.estimated_pregnancy_date) and a.estimated_pregnancy_date
    left join encounter c on a.mother_patid = c.patid
     and c.admit_date between dateadd(year, -1, a.estimated_pregnancy_date) and a.estimated_pregnancy_date
    group by a.birthid
)

select 
  birthid,
  case 
    when admit_counts = 0 then null
    when order_counts <= 5 then 1  -- somewhat arbitrary here
    else 2 end as pre_rx
from renamed