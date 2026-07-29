-- A ranking-eligible row must be based on declared, consistent salary and good geo.
select *
from {{ ref('fct_viabilidade_vagas') }}
where elegivel_ranking
  and (
      fonte_salario <> 'declarado'
      or not coalesce(salario_declarado_consistente, false)
      or qualidade_ivf <> 'alta'
      or geo_mapping_method not in ('exact', 'satellite')
      or geo_confidence < 0.8
  )
