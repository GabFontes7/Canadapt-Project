-- Job Bank listings in the latest Silver snapshot must carry mobility evidence.
with latest as (
    select max(data_snapshot) as data_snapshot
    from {{ ref('stg_adzuna_jobs') }}
)

select s.*
from {{ ref('stg_adzuna_jobs') }} as s
cross join latest as l
where s.data_snapshot = l.data_snapshot
  and s.fonte_vaga = 'jobbank'
  and (
      s.sinais_mobilidade is null
      or trim(s.sinais_mobilidade) in ('', '[]')
  )
