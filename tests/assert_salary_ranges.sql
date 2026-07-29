-- Faixas declaradas e de referência governamental devem estar ordenadas.
select *
from {{ ref('fct_viabilidade_vagas') }}
where (
    salario_declarado_na_vaga is not null
    and salario_declarado_maximo_na_vaga is not null
    and salario_declarado_maximo_na_vaga < salario_declarado_na_vaga
)
or (
    salario_referencia_governo_minimo is not null
    and salario_referencia_governo_maximo is not null
    and salario_referencia_governo_maximo < salario_referencia_governo_minimo
)
