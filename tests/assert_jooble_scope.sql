-- Jooble is intentionally restricted to the two approved professional areas
-- and requires explicit mobility evidence in every retained listing.
select *
from {{ ref('stg_adzuna_jobs') }}
where fonte_vaga = 'jooble'
  and (
      area_foco_coleta not in ('technology', 'banking_operations')
      or sinais_mobilidade is null
      or trim(sinais_mobilidade) in ('', '[]')
  )
