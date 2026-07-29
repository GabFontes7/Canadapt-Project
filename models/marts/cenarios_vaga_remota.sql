-- Cenários estimados para vagas remotas: 1 linha por (vaga × cidade).
-- A vaga permanece remota; o salário estimado da Gold é reavaliado sob
-- o custo e as alíquotas de cada cidade. Tudo é ESTIMATIVA.

with remotas as (
    select
        vaga_id,
        titulo_cargo,
        empresa,
        url_vaga,
        data_criacao,
        extracted_at_utc,
        pipeline_run_id,
        first_seen_at,
        last_seen_at,
        is_current,
        codigo_profissao,
        nome_profissao,
        senioridade_estimada,
        confianca_profissao,
        familia_profissional,
        metodo_localizacao,
        confianca_localizacao,
        salario_bruto_anual_estimado as salario_bruto_anual,
        salario_foi_estimado,
        origem_salario,
        confianca_salario_estimada,
        aviso_salario,
        salario_declarado_consistente
    from {{ ref('fct_viabilidade_vagas') }}
    where metodo_localizacao in ('country_generic', 'remote')
       or upper(coalesce(cidade, '')) = 'REMOTE'
),

cidades_ancora as (
    select
        sk_geografia,
        nome_cidade,
        sigla_provincia,
        nome_provincia,
        aluguel_medio_1bdr,
        custo_vida_sem_aluguel,
        aliquota_hst_total,
        fonte_moradia_url,
        fonte_custo_vida_url,
        metodologia_versao as metodologia_custo_vida,
        'cidade_ancora' as tipo_cenario
    from {{ ref('dim_geografia_custos') }}
    where upper(nome_cidade) <> 'REMOTE'
),

referencia_nacional as (
    select
        sk_geografia,
        nome_cidade,
        sigla_provincia,
        nome_provincia,
        aluguel_medio_1bdr,
        custo_vida_sem_aluguel,
        aliquota_hst_total,
        fonte_moradia_url,
        fonte_custo_vida_url,
        metodologia_versao as metodologia_custo_vida,
        'referencia_nacional' as tipo_cenario
    from {{ ref('dim_geografia_custos') }}
    where upper(nome_cidade) = 'REMOTE'
),

cenarios_geo as (
    select * from cidades_ancora
    union all
    select * from referencia_nacional
),

base as (
    select
        r.*,
        g.sk_geografia as sk_geografia_cenario,
        g.nome_cidade as cidade_cenario,
        g.sigla_provincia as provincia_cenario,
        g.nome_provincia as nome_provincia_cenario,
        g.aluguel_medio_1bdr,
        g.custo_vida_sem_aluguel,
        g.aliquota_hst_total,
        g.fonte_moradia_url,
        g.fonte_custo_vida_url,
        g.metodologia_custo_vida,
        g.tipo_cenario,
        case
            when g.tipo_cenario = 'referencia_nacional' then 'ON'
            else g.sigla_provincia
        end as provincia_fiscal
    from remotas r
    cross join cenarios_geo g
),

fiscal_bruto as (
    select
        b.*,
        (
            select sum(
                greatest(
                    least(b.salario_bruto_anual, coalesce(tb.upper_threshold, b.salario_bruto_anual))
                    - tb.lower_threshold,
                    0
                ) * tb.rate
            )
            from {{ ref('tax_brackets_2026') }} tb
            where tb.jurisdiction = 'FED'
        ) as imposto_federal_bruto,
        (
            select sum(
                greatest(
                    least(b.salario_bruto_anual, coalesce(tb.upper_threshold, b.salario_bruto_anual))
                    - tb.lower_threshold,
                    0
                ) * tb.rate
            )
            from {{ ref('tax_brackets_2026') }} tb
            where tb.jurisdiction = 'PROV'
              and tb.province_code = b.provincia_fiscal
        ) as imposto_provincial_bruto
    from base b
),

contribuicoes as (
    select
        *,
        case
            when salario_bruto_anual is null then null
            when provincia_fiscal = 'QC' then
                least(greatest(salario_bruto_anual - 3500, 0), 71100) * 0.063
                + least(greatest(salario_bruto_anual - 74600, 0), 10400) * 0.04
            else
                least(greatest(salario_bruto_anual - 3500, 0), 71100) * 0.0595
                + least(greatest(salario_bruto_anual - 74600, 0), 10400) * 0.04
        end as contribuicao_cpp_qpp,
        case
            when salario_bruto_anual is null then null
            when provincia_fiscal = 'QC' then least(salario_bruto_anual, 68900) * 0.0127
            else least(salario_bruto_anual, 68900) * 0.0163
        end as premio_ei,
        case
            when salario_bruto_anual is null then null
            when provincia_fiscal = 'QC' then least(salario_bruto_anual, 103000) * 0.0043
            else 0
        end as premio_qpip
    from fiscal_bruto
),

fiscal_creditos as (
    select
        c.*,
        p.basic_personal_amount as valor_pessoal_basico_provincial,
        p.lowest_rate as aliquota_credito_provincial,
        least(
            coalesce(imposto_federal_bruto, 0),
            (
                16452
                + least(coalesce(contribuicao_cpp_qpp, 0), 4230.45)
                + coalesce(premio_ei, 0)
                + 1501
            ) * 0.14
        ) as credito_federal_estimado,
        least(
            coalesce(imposto_provincial_bruto, 0),
            (
                coalesce(p.basic_personal_amount, 0)
                + least(coalesce(contribuicao_cpp_qpp, 0), 4230.45)
                + coalesce(premio_ei, 0)
            ) * coalesce(p.lowest_rate, 0)
        ) as credito_provincial_estimado
    from contribuicoes c
    left join {{ ref('tax_parameters_2026') }} p
        on c.provincia_fiscal = p.province_code
),

liquido as (
    select
        *,
        greatest(
            (coalesce(imposto_federal_bruto, 0) - credito_federal_estimado)
                * case when provincia_fiscal = 'QC' then 0.835 else 1 end,
            0
        ) as imposto_federal_estimado,
        greatest(
            coalesce(imposto_provincial_bruto, 0) - credito_provincial_estimado,
            0
        ) as imposto_provincial_estimado,
        salario_bruto_anual
            - greatest(
                (coalesce(imposto_federal_bruto, 0) - credito_federal_estimado)
                    * case when provincia_fiscal = 'QC' then 0.835 else 1 end,
                0
            )
            - greatest(
                coalesce(imposto_provincial_bruto, 0) - credito_provincial_estimado,
                0
            )
            - coalesce(contribuicao_cpp_qpp, 0)
            - coalesce(premio_ei, 0)
            - coalesce(premio_qpip, 0) as salario_liquido_anual
    from fiscal_creditos
),

cenarios as (
    select
        *,
        case
            when salario_liquido_anual is null then null
            else salario_liquido_anual / 12
        end as poder_compra_mensal,
        case
            when aluguel_medio_1bdr is null or custo_vida_sem_aluguel is null then null
            else aluguel_medio_1bdr + custo_vida_sem_aluguel
        end as custo_total_mensal
    from liquido
),

ranked as (
    select
        *,
        case
            when poder_compra_mensal is null or custo_total_mensal is null then null
            else round(poder_compra_mensal - custo_total_mensal, 0)
        end as sobra_mensal,
        case
            when salario_bruto_anual is null then 'Sem Dados Salariais'
            when poder_compra_mensal is null or custo_total_mensal is null
                then 'Sem Dados Salariais'
            when (poder_compra_mensal - custo_total_mensal) > 1500 then 'Prosperidade'
            when (poder_compra_mensal - custo_total_mensal) >= 0 then 'Equilíbrio'
            else 'Risco Financeiro'
        end as classificacao_bruta,
        case
            when tipo_cenario = 'referencia_nacional' then null
            when poder_compra_mensal is null or custo_total_mensal is null then null
            else row_number() over (
                partition by vaga_id, tipo_cenario
                order by (poder_compra_mensal - custo_total_mensal) desc nulls last
            )
        end as ranking_cidade_na_vaga
    from cenarios
)

select
    {{ dbt_utils.generate_surrogate_key(['vaga_id', 'cidade_cenario', 'provincia_cenario', 'tipo_cenario']) }}
        as sk_cenario_remoto,
    vaga_id,
    titulo_cargo,
    empresa,
    url_vaga,
    data_criacao,
    extracted_at_utc,
    pipeline_run_id,
    first_seen_at,
    last_seen_at,
    is_current,
    codigo_profissao,
    nome_profissao,
    senioridade_estimada,
    confianca_profissao,
    familia_profissional,
    metodo_localizacao,
    confianca_localizacao,
    true as eh_estimativa,
    salario_bruto_anual as salario_bruto_anual_estimado,
    salario_foi_estimado,
    origem_salario,
    confianca_salario_estimada,
    aviso_salario,
    salario_declarado_consistente,
    tipo_cenario,
    sk_geografia_cenario,
    cidade_cenario,
    provincia_cenario,
    nome_provincia_cenario,
    provincia_fiscal,
    round(salario_liquido_anual, 0) as salario_liquido_anual_estimado,
    round(poder_compra_mensal, 0) as poder_compra_mensal_estimado,
    round(aluguel_medio_1bdr, 0) as aluguel_1quarto_estimado,
    round(custo_vida_sem_aluguel, 0) as custo_sem_aluguel_estimado,
    round(custo_total_mensal, 0) as custo_total_mensal_estimado,
    fonte_moradia_url,
    fonte_custo_vida_url,
    metodologia_custo_vida,
    'cra_progressivo_2026_v2' as modelo_fiscal,
    'ESTIMATIVA. Remoto no Canadá — o custo depende de onde você vai morar. Não é aconselhamento financeiro, fiscal ou migratório.' as aviso_cenario_remoto,
    sobra_mensal as sobra_mensal_estimada,
    case
        when classificacao_bruta = 'Sem Dados Salariais'
            then 'Estimativa indisponivel — sem dados salariais'
        else 'Estimativa — ' || classificacao_bruta
    end as classificacao_viabilidade_estimada,
    ranking_cidade_na_vaga,
    (
        tipo_cenario = 'cidade_ancora'
        and origem_salario = 'declarado_na_vaga'
        and coalesce(salario_declarado_consistente, false)
        and sobra_mensal is not null
    ) as ranking_confiavel_remoto
from ranked
