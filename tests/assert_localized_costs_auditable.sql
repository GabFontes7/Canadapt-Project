select *
from {{ ref('fct_viabilidade_vagas') }}
where metodo_localizacao in ('exact', 'satellite')
  and (
      qualidade_custo_vida <> 'auditavel'
      or fonte_moradia_url not like '%cmhc-schl.gc.ca%'
      or fonte_custo_vida_url not like '%statcan.gc.ca%'
      or fator_domicilio_unipessoal is null
      or fator_cpi_2026 is null
  )
