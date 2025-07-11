{% macro vital_features_macro(date1, date2) %}

with cohort as (
    select
        *
    from {{ ref('int_cohort') }}
),

vital as (
    select
        patid,
        {{ add_time_to_date_macro("measure_date", "measure_time") }} as measure_date,
        systolic,
        diastolic,
        wt,
        original_bmi
    from {{ ref('vital') }}
),

height as (
    select 
        birthid,
        mother_height
    from {{ ref('int_mother_height') }}
),

renamed as (
    select
        distinct 
        cohort.birthid,

        avg(b.systolic) over (partition by cohort.birthid) as 'sbp_value_mean',
        avg(b.diastolic) over (partition by cohort.birthid) as 'dbp_value_mean',
        max(b.systolic) over (partition by cohort.birthid) as 'sbp_value_max',
        max(b.diastolic) over (partition by cohort.birthid) as 'dbp_value_max',
        min(b.systolic) over (partition by cohort.birthid) as 'sbp_vale_min',
        min(b.diastolic) over (partition by cohort.birthid) as 'dbp_value_min',
        percentile_cont(0.5) within group (order by b.systolic) over (partition by cohort.birthid) as 'sbp_value_median',
        percentile_cont(0.5) within group (order by b.diastolic) over (partition by cohort.birthid) as 'dbp_value_median',

        -- the MAP mean blood pressure has a lot of null but can be computed from sbp and dbp
        avg(b.systolic/3.0+2.0*b.diastolic/3.0)  over (partition by cohort.birthid) as 'computed_map_value_mean',
        max(b.systolic/3.0+2.0*b.diastolic/3.0)  over (partition by cohort.birthid) as 'computed_map_value_max',
        min(b.systolic/3.0+2.0*b.diastolic/3.0)  over (partition by cohort.birthid) as 'computed_map_value_min',
        percentile_cont(0.5) within group (order by (b.systolic/3.0+2.0*b.diastolic/3.0)) over (partition by cohort.birthid) as 'computed_map_value_median',

        avg(b.systolic - b.diastolic) over (partition by cohort.birthid) as 'pulse_pressure_mean',
        max(b.systolic - b.diastolic) over (partition by cohort.birthid) as 'pulse_pressure_max',
        min(b.systolic - b.diastolic) over (partition by cohort.birthid) as 'pulse_pressure_min',
        percentile_cont(0.5) within group (order by b.systolic - b.diastolic) over (partition by cohort.birthid) as 'pulse_pressure_median',

        avg(b.wt) over (partition by cohort.birthid) as 'wt_mean',
        max(b.wt) over (partition by cohort.birthid) as 'wt_max',
        min(b.wt) over (partition by cohort.birthid) as 'wt_min',
        percentile_cont(0.5) within group (order by b.wt) over (partition by cohort.birthid) as 'wt_median',

        avg(b.original_bmi) over (partition by cohort.birthid) as 'original_bmi_mean',
        max(b.original_bmi) over (partition by cohort.birthid) as 'original_bmi_max',
        min(b.original_bmi) over (partition by cohort.birthid) as 'original_bmi_min',
        percentile_cont(0.5) within group (order by b.original_bmi) over (partition by cohort.birthid) as 'original_bmi_median',

        avg(b.wt/square(c.mother_height)*703) over (partition by cohort.birthid) as 'computed_bmi_mean',
        max(b.wt/square(c.mother_height)*703) over (partition by cohort.birthid) as 'computed_bmi_max',
        min(b.wt/square(c.mother_height)*703) over (partition by cohort.birthid) as 'computed_bmi_min',
        percentile_cont(0.5) within group (order by (b.wt/square(c.mother_height)*703)) over (partition by cohort.birthid) as 'computed_bmi_median'

    from cohort
    left join vital b on cohort.mother_patid = b.patid
     and b.measure_date between {{ date1 }} and {{ date2 }}
    left join height c on cohort.birthid = c.birthid
)

select * from renamed

{% endmacro %}