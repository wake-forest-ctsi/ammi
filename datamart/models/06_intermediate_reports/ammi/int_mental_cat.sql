with cohort as (
    select
        birthid,
        mother_patid,
        estimated_preg_start_date,
        baby_birth_date,
        delivery_admit_date,
        delivery_discharge_date
    from {{ ref('int_cohort') }}
),

dx_list as (
    select
        patid,
        dx_date,
        dx,
        encounterid,
        enc_type,
        case
            when dx in ('F32.0', 'F32.1', 'F32.2', 'F32.3', 'F32.4', 'F32.5', 'F32.8', 
            'F32.89', 'F32.9', 'F32.A', 'F33.0', 'F33.1', 'F33.2', 'F33.3', 'F33.4',
            'F33.40', 'F33.41', 'F33.42', 'F33.8', 'F33.9', 'F34.1', 'F53.0', 'O90.6', 'O99.34') then 'depression'
            when dx in ('F30.10', 'F30.11', 'F30.12', 'F30.13', 'F30.2', 'F30.3', 'F30.4', 'F30.8',
                'F30.9', 'F31.0', 'F31.10', 'F31.11', 'F31.12', 'F31.13', 'F31.2', 'F31.30', 'F31.31', 
                'F31.32', 'F31.4', 'F31.5', 'F31.60', 'F31.61', 'F31.62', 'F31.63', 'F31.64', 'F31.70',
                'F31.71', 'F31.72', 'F31.73', 'F31.74', 'F31.75', 'F31.76', 'F31.77', 'F31.78', 'F31.81',
                'F31.89', 'F31.9', 'F34.0', 'F34.81', 'F34.89', 'F39') then 'bipolar_disorder'
            when dx in ('F06.4', 'F40.9', 'F40.01', 'F40.01', 'F40.02', 'F40.10', 'F40.11',
                'F40.218', 'F40.240', 'F40.241', 'F40.8', 'F41.0', 'F41.1', 'F41.3', 'F41.8', 
                'F41.9', 'F43.0', 'F48.8', 'F48.9', 'F93.8', 'F99', 'R45.7') then 'anxiety'
            when dx in ('F43.10', 'F43.11', 'F43.12') then 'posttraumatic_stress_disorder '
            when dx in ('F42.2', 'F42.3', 'F42.4', 'F42.8', 'F42.9', 'R46.81') then 'obsessive_compulsive_disorder'
            when dx in ('F06.2', 'F06.0', 'F20.0', 'F20.1', 'F20.2', 'F20.3', 'F20.5', 'F20.81',
                'F20.89', 'F20.9', 'F21', 'F22', 'F23', 'F24', 'F25.0', 'F25.1', 'F25.8',
                'F25.9', 'F28', 'F29', 'F53.1', 'F44.0', 'F44.1', 'F44.2', 'F44.81', 'F44.89', 
                'F44.9', 'F48.1', 'F25.1') then 'psychosis'
            when dx in ('R45.850', 'R45.851', 'F06.1', 'F06.30', 'F06.31', 'F06.32', 'F06.33',
                'F06.34', 'F34.9', 'F43.20', 'F43.21', 'F43.22', 'F43.23', 'F43.24', 'F43.25',
                'F43.29', 'F43.81', 'F43.89', 'F43.9', 'F44.4', 'F44.5', 'F44.6', 'F44.7', 'F45.0', 
                'F45.1', 'F45.20', 'F45.21', 'F45.22', 'F45.29', 'F45.41', 'F45.42', 'F45.8', 'F45.9',
                'F51.01', 'F51.02', 'F51.03', 'F51.04', 'F51.05', 'F51.09', 'F51.11', 'F51.12', 'F51.13',
                'F51.19', 'F51.8', 'F51.9', 'F54', 'F59', 'O99.340', 'O99.341', 'O99.342', 'O99.343',
                'O99.344', 'O99.345') then 'other'
            else null end as mental_cat
    from {{ ref('diagnosis') }}
),

renamed as (
    select
        a.birthid,
        a.mother_patid,
        a.baby_birth_date,
        a.delivery_admit_date,
        a.delivery_discharge_date,
        b.dx_date,
        b.dx,
        b.encounterid,
        b.enc_type,
        datediff(day, a.estimated_preg_start_date, b.dx_date) as gestage_days,
        mental_cat
    from cohort a
    left join dx_list b on a.mother_patid = b.patid
     and b.mental_cat is not null
)

select * from renamed