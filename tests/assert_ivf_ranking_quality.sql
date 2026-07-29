-- Ranking confiável exige salário declarado na vaga e cálculo de alta confiança.
select *
from {{ ref('fct_viabilidade_vagas') }}
where ranking_confiavel
  and (
      origem_salario <> 'declarado_na_vaga'
      or not coalesce(salario_declarado_consistente, false)
      or confianca_calculo <> 'alta'
      or metodo_localizacao not in ('exact', 'satellite')
      or confianca_localizacao < 0.8
  )
