select
    a.birthid,
    case
      when a.acs_median_hh_inc is null then null
      when a.acs_median_hh_inc <= 15000 then 1
      when a.acs_median_hh_inc <= 19000 then 2
      when a.acs_median_hh_inc <= 22000 then 3
      when a.acs_median_hh_inc <= 26000 then 4
      when a.acs_median_hh_inc <= 29000 then 5
      when a.acs_median_hh_inc <= 37000 then 6
      when a.acs_median_hh_inc <= 44000 then 7
      when a.acs_median_hh_inc <= 52000 then 8
      when a.acs_median_hh_inc <= 56000 then 9
      when a.acs_median_hh_inc <= 67000 then 10
      when a.acs_median_hh_inc <= 79000 then 11
      when a.acs_median_hh_inc >  79000 then 12
      else null end
    as income7
from {{ ref('int_censustract_features') }} a