-- Cada vaga remota deve ter exatamente 17 cidades-âncora + 1 referência nacional.
with contagem as (
    select
        vaga_id,
        count(*) as n_cenarios,
        count(*) filter (where tipo_cenario = 'cidade_ancora') as n_ancoras,
        count(*) filter (where tipo_cenario = 'referencia_nacional') as n_ref
    from {{ ref('cenarios_vaga_remota') }}
    group by vaga_id
)

select *
from contagem
where n_ancoras <> 17
   or n_ref <> 1
   or n_cenarios <> 18
