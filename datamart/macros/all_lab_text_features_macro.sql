{% macro all_lab_text_features_macro(cohort_table, min_count, date1, date2) %}

with cohort as (
    select
        *
    from {{ cohort_table }}
),

lab_text as (
    select
        patid,
        lab_loinc,
        {{ add_time_to_date_macro('specimen_date', 'specimen_time') }} specimen_date,
        raw_lab_name as lab_name,
        raw_result as result
    from {{ ref('lab_result_cm') }}
    where result_modifier = 'TX' 
      and lab_loinc != '31208-2'  -- 'Specimen source identified'
      and lab_loinc != '8251-1'   -- 'Service comment'
      and lab_loinc != '34970-4'  -- 'Ultrasound date'
      and lab_loinc != '2345-7'   -- seems specify 'Glucose [Mass/volume] in Serum or Plasma'
      and lab_loinc != '49549-9'  -- 'Referral lab test method'
      and lab_loinc != '62364-5'  -- 'Test performance information in Specimen Narrative'
      and lab_loinc != '72486-4'  -- 'Laboratory director name in Provider
      and lab_loinc != '75608-0'  -- 'Citation [Bibliographic Citation] in Referral lab test Narrative'
      and lab_loinc != '77202-0'  -- 'Laboratory comment [Text] in Report Narrative'
),

lab_selected as (
    select
        cohort.birthid,
        cohort.mother_patid,
        cohort.baby_birth_date,
        lab_loinc,
        specimen_date,
        lab_name,
        result
    from cohort
    inner join lab_text on cohort.mother_patid = lab_text.patid 
     and lab_text.specimen_date between {{ date1 }} and {{ date2 }}
),

lab_count as (
    select
        lab_loinc
    from lab_selected
    group by lab_loinc
    having count(distinct birthid) >= {{ min_count }}
),

renamed as (
    select
        lab_selected.birthid,
        lab_selected.mother_patid,
        lab_selected.baby_birth_date,
        lab_selected.lab_loinc,
        lab_selected.specimen_date,
        lab_selected.lab_name,
        lab_selected.result
    from lab_selected
    inner join lab_count on lab_selected.lab_loinc = lab_count.lab_loinc
)

select * from renamed

{% endmacro %}