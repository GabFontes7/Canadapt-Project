-- Referência nacional nunca entra no ranking remoto.
select *
from {{ ref('cenarios_vaga_remota') }}
where tipo_cenario = 'referencia_nacional'
  and (
      ranking_confiavel_remoto
      or ranking_cidade_na_vaga is not null
  )
