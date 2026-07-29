with fato as (
    select * from "canadapt_analytics"."main"."fato_vagas_visto"
),

dim as (
    select * from "canadapt_analytics"."main"."dim_geografia_custos"
),

base as (
    select
        f.vaga_id,
        f.titulo_cargo,
        f.empresa,
        f.salario_bruto_anual,
        d.nome_cidade,
        d.sigla_provincia,
        d.nome_provincia,
        d.aluguel_medio_1bdr,
        d.custo_vida_sem_aluguel,
        d.aliquota_hst_total,
        case
            when f.salario_bruto_anual <= 55000 then 0.15
            when f.salario_bruto_anual <= 110000 then 0.22
            else 0.29
        end as aliquota_imposto_renda_combinado
    from fato f
    left join dim d
        on f.sk_geografia = d.sk_geografia
),

liquido as (
    select
        *,
        (salario_bruto_anual * (1 - aliquota_imposto_renda_combinado)) as salario_liquido_anual
    from base
),

poder as (
    select
        *,
        ((salario_liquido_anual / 12) * (1 - (0.30 * aliquota_hst_total))) as poder_compra_real_mensal,
        (aluguel_medio_1bdr + custo_vida_sem_aluguel) as custo_total_mensal
    from liquido
)

select
    vaga_id,
    titulo_cargo,
    empresa,
    salario_bruto_anual,
    nome_cidade,
    sigla_provincia,
    nome_provincia,
    aliquota_imposto_renda_combinado,
    salario_liquido_anual,
    poder_compra_real_mensal,
    aluguel_medio_1bdr,
    custo_vida_sem_aluguel,
    custo_total_mensal,
    (poder_compra_real_mensal - custo_total_mensal) as ivf_score,
    case
        when (poder_compra_real_mensal - custo_total_mensal) > 1500 then 'Prosperidade'
        when (poder_compra_real_mensal - custo_total_mensal) >= 0 then 'Equilíbrio'
        else 'Risco Financeiro'
    end as classificacao_viabilidade
from poder