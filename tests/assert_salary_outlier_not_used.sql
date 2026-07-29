-- Referências governamentais atípicas são auditadas, mas nunca alimentam
-- o cálculo de salário/sobra mensal.
select *
from {{ ref('fct_viabilidade_vagas') }}
where salario_referencia_governo_atipico
  and origem_salario in (
      'estimado_governo_provincia',
      'estimado_governo_nacional'
  )
