-- reproduce kibria medication list
with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

prescribing as (
    select
        patid,
        raw_rx_med_name,
        {{ add_time_to_date_macro("rx_order_date", "rx_order_time") }} as rx_order_date
    from {{ ref('prescribing') }}
),

renamed as (
    select
        cohort.birthid,
        max(case when lower(b.raw_rx_med_name) like '%aspirin%' then 1 else 0 end) as "med_rx_aspirin",
        max(case when lower(b.raw_rx_med_name) like '%nifedipine%' then 1 else 0 end) as "med_rx_nifedipine",
        max(case when lower(b.raw_rx_med_name) like '%insulin%' then 1 else 0 end) as "med_rx_insulin",
        max(case when lower(b.raw_rx_med_name) like '%amlodipine%' then 1 else 0 end) as "med_rx_amlodipine",
        max(case when lower(b.raw_rx_med_name) like '%glucagon%' then 1 else 0 end) as "med_rx_glucagon",
        max(case when lower(b.raw_rx_med_name) like '%glucose%' then 1 else 0 end) as "med_rx_glucose",
        max(case when lower(b.raw_rx_med_name) like '%methyldopa%' then 1 else 0 end) as "med_rx_methyldopa",
        max(case when lower(b.raw_rx_med_name) like '%promethazine%' then 1 else 0 end) as "med_rx_promethazine",
        max(case when lower(b.raw_rx_med_name) like '% ethyl %' then 1 else 0 end) as "med_rx_ethyl",
        max(case when lower(b.raw_rx_med_name) like '%famotidine%' then 1 else 0 end) as "med_rx_famotidine",
        max(case when lower(b.raw_rx_med_name) like '%ondansetron%' then 1 else 0 end) as "med_rx_ondansetron"
    from cohort
    left join {{ ref('prescribing') }} b on cohort.mother_patid = b.patid
     and b.rx_order_date between cohort.estimated_preg_start_date and dateadd(week, 20, cohort.estimated_preg_start_date)
    group by cohort.birthid
)

select * from renamed