-- Período ambíguo nunca pode ser convertido em salário anual.
select *
from {{ ref('stg_wages_official') }}
where wage_period = 'ambiguous'
  and (
      annualization_factor is not null
      or salary_annual_low is not null
      or salary_annual_median is not null
      or salary_annual_high is not null
  )
