-- Pesquisa salarial só entra no cálculo com URL https e valor encontrado.
select *
from {{ ref('fct_viabilidade_vagas') }}
where origem_salario in (
    'estimado_pesquisa_vaga',
    'estimado_pesquisa_empresa',
    'estimado_pesquisa_web'
)
and (
    not coalesce(pesquisa_salarial_encontrou, false)
    or url_fonte_pesquisa_salarial is null
    or url_fonte_pesquisa_salarial not like 'https://%'
    or salario_pesquisa_anual_estimado is null
)
