-- Declared and official ranges must be ordered when both boundaries exist.
select *
from {{ ref('fct_viabilidade_vagas') }}
where (
    salario_declarado is not null
    and salario_declarado_maximo is not null
    and salario_declarado_maximo < salario_declarado
)
or (
    salario_oficial_minimo is not null
    and salario_oficial_maximo is not null
    and salario_oficial_maximo < salario_oficial_minimo
)
