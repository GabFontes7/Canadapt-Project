with fato as (
    select * from {{ ref('fato_vagas_visto') }}
),

dim as (
    select * from {{ ref('dim_geografia_custos') }}
),

wages_provincia as (
    select *
    from {{ ref('stg_wages_official') }}
    where province_code <> 'NAT'
      and length(economic_region_code) = 4
),

wages_nacional as (
    select *
    from {{ ref('stg_wages_official') }}
    where province_code = 'NAT'
      and economic_region_code = 'ER00'
),

-- Base: vaga + geografia (salário ainda só o declarado pela Adzuna)
joined as (
    select
        f.vaga_id,
        f.titulo_cargo,
        f.empresa,
        f.noc_code,
        f.noc_title,
        f.seniority,
        f.noc_confidence,
        f.cma_padronizada,
        f.geo_mapping_method,
        f.geo_confidence,
        f.salario_bruto_anual as salario_declarado,
        f.url_vaga,
        f.data_criacao,
        f.first_seen_at,
        f.last_seen_at,
        d.nome_cidade,
        d.sigla_provincia,
        d.nome_provincia,
        d.aluguel_medio_1bdr,
        d.custo_vida_sem_aluguel,
        d.aliquota_hst_total,
        coalesce(wp.salary_annual_low, wn.salary_annual_low)
            as salario_oficial_minimo,
        coalesce(wp.salary_annual_median, wn.salary_annual_median)
            as salario_oficial_mediano,
        coalesce(wp.salary_annual_high, wn.salary_annual_high)
            as salario_oficial_maximo,
        coalesce(wp.salary_source, wn.salary_source) as fonte_salario_oficial,
        coalesce(wp.dataset_reference_year, wn.dataset_reference_year)
            as ano_referencia_salario_oficial,
        case
            when wp.salary_annual_median is not null then 'provincia'
            when wn.salary_annual_median is not null then 'nacional'
        end as abrangencia_salario_oficial
    from fato f
    left join dim d
        on f.sk_geografia = d.sk_geografia
    left join wages_provincia wp
        on f.noc_code = wp.noc_code
       and d.sigla_provincia = wp.province_code
    left join wages_nacional wn
        on f.noc_code = wn.noc_code
),

-- Famílias amplas evitam depender de títulos idênticos e esparsos.
classificado as (
    select
        *,
        case
            when salario_declarado between 20000 and 500000 then salario_declarado
            else null
        end as salario_declarado_validado,
        case
            when salario_declarado is null then null
            when salario_declarado between 20000 and 500000 then true
            else false
        end as salario_declarado_consistente,
        case
            when regexp_matches(lower(titulo_cargo), 'nurse|(^|[^a-z])rn([^a-z]|$)|medical|health|patient|mri|medicine')
                then 'saude'
            when regexp_matches(lower(titulo_cargo), 'engineer|developer|software|data|analytics|analyst|technolog|java|\\.net|azure|devops|machine learning|ai ')
                then 'tecnologia_engenharia'
            when regexp_matches(lower(titulo_cargo), 'sales|marketing|canvass|business development')
                then 'vendas_marketing'
            when regexp_matches(lower(titulo_cargo), 'operations|logistics|purchasing|supply chain|technician|maintenance|driver|operator')
                then 'operacoes_logistica'
            when regexp_matches(lower(titulo_cargo), 'product|project|scrum|product owner')
                then 'produto_projetos'
            when regexp_matches(lower(titulo_cargo), 'lecturer|tutor|teacher|educational|school|faculty')
                then 'educacao'
            when regexp_matches(lower(titulo_cargo), 'finance|investment|compliance|regulatory|accounting')
                then 'financas_conformidade'
            when regexp_matches(lower(titulo_cargo), 'manager|director|head|consultant|lead')
                then 'gestao_consultoria'
            else 'outros'
        end as familia_cargo
    from joined
),

-- Valores fora desta faixa não são usados como referência anual.
benchmark_source as (
    select *
    from classificado
    where salario_declarado_validado is not null
),

-- Benchmarks a partir das próprias vagas com salário anual plausível.
bench_empresa as (
    select
        empresa,
        median(salario_declarado_validado) as salario_mediano_empresa,
        count(*) as n_empresa
    from benchmark_source
    where empresa is not null
      and trim(empresa) <> ''
    group by empresa
    having count(*) >= 3
),

bench_cargo_provincia as (
    select
        familia_cargo,
        sigla_provincia,
        median(salario_declarado_validado) as salario_mediano_cargo_provincia,
        count(*) as n_cargo_provincia
    from benchmark_source
    where sigla_provincia is not null
    group by familia_cargo, sigla_provincia
    having count(*) >= 2
),

bench_cargo_nacional as (
    select
        familia_cargo,
        median(salario_declarado_validado) as salario_mediano_cargo_nacional,
        count(*) as n_cargo_nacional
    from benchmark_source
    group by familia_cargo
    having count(*) >= 3
),

bench_provincia as (
    select
        sigla_provincia,
        median(salario_declarado_validado) as salario_mediano_provincia,
        count(*) as n_provincia
    from benchmark_source
    where sigla_provincia is not null
    group by sigla_provincia
    having count(*) >= 2
),

bench_nacional as (
    select
        median(salario_declarado_validado) as salario_mediano_nacional,
        count(*) as n_nacional
    from benchmark_source
),

-- Cascata: declarado > ESDC NOC/região > ESDC NOC nacional
-- > benchmarks internos.
estimado as (
    select
        j.*,
        bcp.salario_mediano_cargo_provincia,
        be.salario_mediano_empresa,
        bcn.salario_mediano_cargo_nacional,
        bp.salario_mediano_provincia,
        bn.salario_mediano_nacional,
        coalesce(
            j.salario_declarado_validado,
            j.salario_oficial_mediano,
            bcp.salario_mediano_cargo_provincia,
            be.salario_mediano_empresa,
            bcn.salario_mediano_cargo_nacional,
            bp.salario_mediano_provincia,
            bn.salario_mediano_nacional
        ) as salario_bruto_anual,
        case
            when j.salario_declarado_validado is not null then false
            when coalesce(
                j.salario_oficial_mediano,
                bcp.salario_mediano_cargo_provincia,
                be.salario_mediano_empresa,
                bcn.salario_mediano_cargo_nacional,
                bp.salario_mediano_provincia,
                bn.salario_mediano_nacional
            ) is not null then true
            else null
        end as salario_estimado,
        case
            when j.salario_declarado_validado is not null then 'declarado'
            when j.salario_oficial_mediano is not null
              and j.abrangencia_salario_oficial = 'provincia'
                then 'oficial_noc_provincia'
            when j.salario_oficial_mediano is not null
                then 'oficial_noc_nacional'
            when bcp.salario_mediano_cargo_provincia is not null then 'mercado_cargo_provincia'
            when be.salario_mediano_empresa is not null then 'mercado_empresa'
            when bcn.salario_mediano_cargo_nacional is not null then 'mercado_cargo_nacional'
            when bp.salario_mediano_provincia is not null then 'mercado_provincia'
            when bn.salario_mediano_nacional is not null then 'mercado_nacional'
            else 'indisponivel'
        end as fonte_salario,
        case
            when j.salario_declarado_validado is not null then 'alta'
            when j.salario_oficial_mediano is not null
              and coalesce(j.noc_confidence, 0) >= 0.75 then 'alta'
            when j.salario_oficial_mediano is not null then 'media'
            when bcp.salario_mediano_cargo_provincia is not null then 'media'
            when be.salario_mediano_empresa is not null then 'media'
            when bcn.salario_mediano_cargo_nacional is not null then 'media'
            when bp.salario_mediano_provincia is not null then 'baixa'
            when bn.salario_mediano_nacional is not null then 'baixa'
            else 'indisponivel'
        end as confianca_salario,
        case
            when j.salario_declarado_validado is not null then null
            when j.salario_declarado is not null then 'valor_declarado_inconsistente'
            else 'salario_nao_informado'
        end as motivo_salario_estimado,
        case
            when j.salario_declarado_validado is not null then null
            when j.salario_oficial_mediano is not null then null
            when bcp.salario_mediano_cargo_provincia is not null then bcp.n_cargo_provincia
            when be.salario_mediano_empresa is not null then
                be.n_empresa
            when bcn.salario_mediano_cargo_nacional is not null then
                bcn.n_cargo_nacional
            when bp.salario_mediano_provincia is not null then
                bp.n_provincia
            when bn.salario_mediano_nacional is not null then
                bn.n_nacional
            else null
        end as tamanho_amostra_salario,
        case
            when j.salario_declarado_validado is not null then null
            when j.salario_oficial_mediano is not null then
                case
                    when j.salario_declarado is not null then '* Valor declarado inconsistente — '
                    else '* Salário não informado — '
                end
                || 'mediana oficial ESDC para NOC ' || j.noc_code
                || ' (' || j.abrangencia_salario_oficial
                || ', referência ' || j.ano_referencia_salario_oficial::varchar || ')'
            when bcp.salario_mediano_cargo_provincia is not null then
                case
                    when j.salario_declarado is not null then '* Valor declarado inconsistente — '
                    else '* Salário não informado — '
                end
                || 'estimativa pela mediana da família '
                || j.familia_cargo || ' em ' || coalesce(j.sigla_provincia, '?')
            when be.salario_mediano_empresa is not null then
                case
                    when j.salario_declarado is not null then '* Valor declarado inconsistente — '
                    else '* Salário não informado — '
                end
                || 'estimativa pela mediana da empresa no dataset'
            when bcn.salario_mediano_cargo_nacional is not null then
                case
                    when j.salario_declarado is not null then '* Valor declarado inconsistente — '
                    else '* Salário não informado — '
                end
                || 'estimativa pela mediana nacional da família '
                || j.familia_cargo
            when bp.salario_mediano_provincia is not null then
                case
                    when j.salario_declarado is not null then '* Valor declarado inconsistente — '
                    else '* Salário não informado — '
                end
                || 'estimativa pela mediana da província ('
                || coalesce(j.sigla_provincia, '?') || ')'
            when bn.salario_mediano_nacional is not null then
                case
                    when j.salario_declarado is not null then '* Valor declarado inconsistente — '
                    else '* Salário não informado — '
                end
                || 'estimativa pela mediana nacional do dataset'
            else
                '* Salário não informado — sem benchmark disponível'
        end as aviso_salario
    from classificado j
    left join bench_cargo_provincia bcp
        on j.familia_cargo = bcp.familia_cargo
       and j.sigla_provincia = bcp.sigla_provincia
    left join bench_empresa be
        on j.empresa = be.empresa
    left join bench_cargo_nacional bcn
        on j.familia_cargo = bcn.familia_cargo
    left join bench_provincia bp
        on j.sigla_provincia = bp.sigla_provincia
    cross join bench_nacional bn
),

base as (
    select
        *,
        case
            when salario_bruto_anual is null then null
            when salario_bruto_anual <= 55000 then 0.15
            when salario_bruto_anual <= 110000 then 0.22
            else 0.29
        end as aliquota_imposto_renda_combinado
    from estimado
),

liquido as (
    select
        *,
        case
            when salario_bruto_anual is null then null
            else salario_bruto_anual * (1 - aliquota_imposto_renda_combinado)
        end as salario_liquido_anual
    from base
),

poder as (
    select
        *,
        case
            when salario_liquido_anual is null or aliquota_hst_total is null then null
            else (salario_liquido_anual / 12) * (1 - (0.30 * aliquota_hst_total))
        end as poder_compra_real_mensal,
        case
            when aluguel_medio_1bdr is null or custo_vida_sem_aluguel is null then null
            else aluguel_medio_1bdr + custo_vida_sem_aluguel
        end as custo_total_mensal
    from liquido
)

select
    vaga_id,
    titulo_cargo,
    empresa,
    url_vaga,
    data_criacao,
    first_seen_at,
    last_seen_at,
    noc_code,
    noc_title,
    seniority,
    noc_confidence,
    cma_padronizada,
    geo_mapping_method,
    geo_confidence,
    salario_declarado,
    salario_declarado_consistente,
    salario_bruto_anual,
    salario_estimado,
    familia_cargo,
    fonte_salario,
    confianca_salario,
    motivo_salario_estimado,
    tamanho_amostra_salario,
    aviso_salario,
    salario_oficial_minimo,
    salario_oficial_mediano,
    salario_oficial_maximo,
    fonte_salario_oficial,
    ano_referencia_salario_oficial,
    abrangencia_salario_oficial,
    nome_cidade,
    sigla_provincia,
    nome_provincia,
    aliquota_imposto_renda_combinado,
    salario_liquido_anual,
    poder_compra_real_mensal,
    aluguel_medio_1bdr,
    custo_vida_sem_aluguel,
    custo_total_mensal,
    'simplificado_v1' as modelo_fiscal,
    case
        when poder_compra_real_mensal is null or custo_total_mensal is null then null
        else poder_compra_real_mensal - custo_total_mensal
    end as ivf_score,
    case
        when poder_compra_real_mensal is null or custo_total_mensal is null then null
        else poder_compra_real_mensal - custo_total_mensal
    end as ivf_score_estimado,
    case
        when salario_bruto_anual is null then 'Sem Dados Salariais'
        when poder_compra_real_mensal is null or custo_total_mensal is null then 'Sem Dados Salariais'
        when (poder_compra_real_mensal - custo_total_mensal) > 1500 then 'Prosperidade'
        when (poder_compra_real_mensal - custo_total_mensal) >= 0 then 'Equilíbrio'
        else 'Risco Financeiro'
    end as classificacao_viabilidade
from poder
