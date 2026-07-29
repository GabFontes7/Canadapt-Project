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
        f.noc_mapping_method,
        f.noc_cache_key,
        f.noc_evidence,
        f.noc_model,
        f.noc_prompt_version,
        f.noc_classified_at_utc,
        f.noc_has_wage_benchmark,
        f.cma_padronizada,
        f.geo_mapping_method,
        f.geo_confidence,
        f.salario_bruto_anual as salario_declarado,
        f.salario_bruto_anual_maximo as salario_declarado_maximo,
        coalesce(f.salario_adzuna_estimado, 0) = 1 as salario_adzuna_predito,
        f.url_vaga,
        f.data_criacao,
        f.extracted_at_utc,
        f.pipeline_run_id,
        f.first_seen_at,
        f.last_seen_at,
        f.is_current,
        coalesce(f.salary_research_found, false) as salary_research_found,
        f.salary_research_annual_min,
        f.salary_research_annual_mid,
        f.salary_research_annual_max,
        f.salary_research_period_original,
        f.salary_research_currency,
        f.salary_research_source_type,
        f.salary_research_source_url,
        f.salary_research_source_title,
        f.salary_research_observed_date,
        f.salary_research_confidence,
        f.salary_research_evidence,
        f.salary_research_rejection_reason,
        f.salary_research_model,
        f.salary_research_prompt_version,
        f.salary_research_at_utc,
        f.salary_research_cache_key,
        d.nome_cidade,
        d.sigla_provincia,
        d.nome_provincia,
        d.aluguel_medio_1bdr,
        d.custo_vida_sem_aluguel,
        d.aliquota_hst_total,
        d.fonte_moradia_url,
        d.fonte_custo_vida_url,
        d.ano_fonte_moradia,
        d.ano_fonte_custo_vida,
        d.qualidade_fonte_moradia,
        d.metodo_custo_vida,
        d.custo_vida_estimado,
        d.custo_base_provincial_2023,
        d.fator_domicilio_unipessoal,
        d.fator_cpi_2026,
        d.cpi_mes_referencia,
        d.metodologia_versao as metodologia_custo_vida,
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

salario_adzuna_normalizado as (
    select
        *,
        case
            when salario_declarado between 20000 and 500000
             and salario_declarado_maximo between 20000 and 500000
             and salario_declarado_maximo >= salario_declarado
                then (salario_declarado + salario_declarado_maximo) / 2
            when salario_declarado between 20000 and 500000
                then salario_declarado
            else null
        end as salario_adzuna_referencia
    from joined
),

-- Famílias amplas evitam depender de títulos idênticos e esparsos.
classificado as (
    select
        *,
        case
            when not salario_adzuna_predito then salario_adzuna_referencia
        end as salario_declarado_validado,
        case
            when salario_adzuna_predito then salario_adzuna_referencia
        end as salario_adzuna_predito_validado,
        case
            when coalesce(salary_research_found, false)
             and salary_research_annual_mid between 20000 and 500000
             and coalesce(salary_research_confidence, 0) >= 0.55
             and salary_research_source_url like 'https://%'
             and salary_research_source_type in (
                 'same_job_posting',
                 'company_careers',
                 'reputable_aggregator'
             )
                then salary_research_annual_mid
        end as salario_pesquisa_validado,
        case
            when salario_declarado is null then null
            when salario_adzuna_referencia is not null then true
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
    from salario_adzuna_normalizado
),

official_family_stats as (
    select
        familia_cargo,
        approx_quantile(salario_oficial_mediano, 0.95) as salario_oficial_p95
    from classificado
    where salario_oficial_mediano is not null
    group by familia_cargo
),

classificado_com_outlier as (
    select
        c.*,
        s.salario_oficial_p95,
        (
            c.salario_oficial_mediano > 300000
            and c.salario_oficial_mediano
                > coalesce(s.salario_oficial_p95 * 1.5, 300000)
        ) as salario_oficial_outlier,
        case
            when c.salario_oficial_mediano > 300000
             and c.salario_oficial_mediano
                > coalesce(s.salario_oficial_p95 * 1.5, 300000)
                then 'salario_oficial_acima_p95_familia'
        end as motivo_outlier_salario
    from classificado c
    left join official_family_stats s using (familia_cargo)
),

referencia_governo_validada as (
    select
        *,
        case
            when coalesce(salario_oficial_outlier, false) then null
            else salario_oficial_mediano
        end as salario_oficial_mediano_validado
    from classificado_com_outlier
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

-- Cascata: declarado > referência governamental validada > Adzuna
-- > benchmarks internos. Referências atípicas nunca entram no cálculo.
estimado as (
    select
        j.*,
        bcp.salario_mediano_cargo_provincia,
        be.salario_mediano_empresa,
        bcn.salario_mediano_cargo_nacional,
        bp.salario_mediano_provincia,
        bn.salario_mediano_nacional,
        try_cast(
            coalesce(
                j.salario_declarado_validado,
                j.salario_pesquisa_validado,
                j.salario_oficial_mediano_validado,
                j.salario_adzuna_predito_validado,
                bcp.salario_mediano_cargo_provincia,
                be.salario_mediano_empresa,
                bcn.salario_mediano_cargo_nacional,
                bp.salario_mediano_provincia,
                bn.salario_mediano_nacional
            ) as double
        ) as salario_bruto_anual,
        case
            when j.salario_declarado_validado is not null then false
            when coalesce(
                j.salario_pesquisa_validado,
                j.salario_oficial_mediano_validado,
                j.salario_adzuna_predito_validado,
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
            when j.salario_pesquisa_validado is not null
              and j.salary_research_source_type = 'same_job_posting'
                then 'pesquisa_vaga_verificada'
            when j.salario_pesquisa_validado is not null
              and j.salary_research_source_type = 'company_careers'
                then 'pesquisa_empresa_verificada'
            when j.salario_pesquisa_validado is not null
                then 'pesquisa_web_verificada'
            when j.salario_oficial_mediano_validado is not null
              and j.abrangencia_salario_oficial = 'provincia'
                then 'oficial_noc_provincia'
            when j.salario_oficial_mediano_validado is not null
                then 'oficial_noc_nacional'
            when j.salario_adzuna_predito_validado is not null then 'adzuna_predito'
            when bcp.salario_mediano_cargo_provincia is not null then 'mercado_cargo_provincia'
            when be.salario_mediano_empresa is not null then 'mercado_empresa'
            when bcn.salario_mediano_cargo_nacional is not null then 'mercado_cargo_nacional'
            when bp.salario_mediano_provincia is not null then 'mercado_provincia'
            when bn.salario_mediano_nacional is not null then 'mercado_nacional'
            else 'indisponivel'
        end as fonte_salario,
        case
            when j.salario_declarado_validado is not null then 'alta'
            when j.salario_pesquisa_validado is not null
              and coalesce(j.salary_research_confidence, 0) >= 0.75 then 'alta'
            when j.salario_pesquisa_validado is not null then 'media'
            when j.salario_oficial_mediano_validado is not null
              and coalesce(j.noc_confidence, 0) >= 0.75 then 'alta'
            when j.salario_oficial_mediano_validado is not null then 'media'
            when j.salario_adzuna_predito_validado is not null then 'media'
            when bcp.salario_mediano_cargo_provincia is not null then 'media'
            when be.salario_mediano_empresa is not null then 'media'
            when bcn.salario_mediano_cargo_nacional is not null then 'media'
            when bp.salario_mediano_provincia is not null then 'baixa'
            when bn.salario_mediano_nacional is not null then 'baixa'
            else 'indisponivel'
        end as confianca_salario,
        case
            when j.salario_declarado_validado is not null then null
            when j.salario_pesquisa_validado is not null then 'salario_pesquisado_com_fonte'
            when j.salario_oficial_mediano_validado is not null then 'salario_nao_informado'
            when j.salario_adzuna_predito_validado is not null then 'salario_predito_adzuna'
            when j.salario_declarado is not null then 'valor_declarado_inconsistente'
            else 'salario_nao_informado'
        end as motivo_salario_estimado,
        case
            when j.salario_declarado_validado is not null then null
            when j.salario_pesquisa_validado is not null then null
            when j.salario_oficial_mediano_validado is not null then null
            when j.salario_adzuna_predito_validado is not null then null
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
            when j.salario_pesquisa_validado is not null then
                '* Salário não informado na vaga — estimativa com fonte verificável: '
                || coalesce(j.salary_research_source_url, 'sem_url')
            when j.salario_oficial_mediano_validado is not null then
                case
                    when j.salario_declarado is not null then '* Valor declarado inconsistente — '
                    else '* Salário não informado — '
                end
                || 'estimativa por referência governamental (ESDC) para a profissão '
                || j.noc_code
                || ' (' || j.abrangencia_salario_oficial
                || ', referência ' || j.ano_referencia_salario_oficial::varchar || ')'
            when j.salario_adzuna_predito_validado is not null then
                '* Faixa salarial estimada pela Adzuna — valor de referência é o ponto médio'
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
    from referencia_governo_validada j
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

fiscal_bruto as (
    select
        e.*,
        (
            select sum(
                greatest(
                    least(e.salario_bruto_anual, coalesce(b.upper_threshold, e.salario_bruto_anual))
                    - b.lower_threshold,
                    0
                ) * b.rate
            )
            from {{ ref('tax_brackets_2026') }} b
            where b.jurisdiction = 'FED'
        ) as imposto_federal_bruto,
        (
            select sum(
                greatest(
                    least(e.salario_bruto_anual, coalesce(b.upper_threshold, e.salario_bruto_anual))
                    - b.lower_threshold,
                    0
                ) * b.rate
            )
            from {{ ref('tax_brackets_2026') }} b
            where b.jurisdiction = 'PROV'
              and b.province_code = e.sigla_provincia
        ) as imposto_provincial_bruto
    from estimado e
),

contribuicoes as (
    select
        *,
        case
            when salario_bruto_anual is null then null
            when sigla_provincia = 'QC' then
                least(greatest(salario_bruto_anual - 3500, 0), 71100) * 0.063
                + least(greatest(salario_bruto_anual - 74600, 0), 10400) * 0.04
            else
                least(greatest(salario_bruto_anual - 3500, 0), 71100) * 0.0595
                + least(greatest(salario_bruto_anual - 74600, 0), 10400) * 0.04
        end as contribuicao_cpp_qpp,
        case
            when salario_bruto_anual is null then null
            when sigla_provincia = 'QC' then least(salario_bruto_anual, 68900) * 0.0127
            else least(salario_bruto_anual, 68900) * 0.0163
        end as premio_ei,
        case
            when salario_bruto_anual is null then null
            when sigla_provincia = 'QC' then least(salario_bruto_anual, 103000) * 0.0043
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
        on c.sigla_provincia = p.province_code
),

liquido as (
    select
        *,
        greatest(
            (coalesce(imposto_federal_bruto, 0) - credito_federal_estimado)
                * case when sigla_provincia = 'QC' then 0.835 else 1 end,
            0
        ) as imposto_federal_estimado,
        greatest(
            coalesce(imposto_provincial_bruto, 0) - credito_provincial_estimado,
            0
        )
            as imposto_provincial_estimado,
        salario_bruto_anual
            - greatest(
                (coalesce(imposto_federal_bruto, 0) - credito_federal_estimado)
                    * case when sigla_provincia = 'QC' then 0.835 else 1 end,
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

poder as (
    select
        *,
        case
            when salario_liquido_anual is null then null
            when geo_mapping_method in ('country_generic', 'remote') then null
            else salario_liquido_anual / 12
        end as poder_compra_real_mensal,
        case
            when aluguel_medio_1bdr is null or custo_vida_sem_aluguel is null then null
            else aluguel_medio_1bdr + custo_vida_sem_aluguel
        end as custo_total_mensal
    from liquido
),

qualidade as (
    select
        *,
        round(poder_compra_real_mensal, 0) as poder_compra_real_mensal_arred,
        round(custo_total_mensal, 0) as custo_total_mensal_arred,
        round(salario_liquido_anual, 0) as salario_liquido_anual_arred,
        round(salario_bruto_anual, 0) as salario_bruto_anual_arred,
        case
            when poder_compra_real_mensal is null or custo_total_mensal is null then null
            else round(poder_compra_real_mensal - custo_total_mensal, 0)
        end as ivf_score_arred,
        case
            when metodologia_custo_vida = 'official_sources_v2'
             and fonte_moradia_url like '%cmhc-schl.gc.ca%'
             and fonte_custo_vida_url like '%statcan.gc.ca%'
             and coalesce(custo_vida_estimado, false)
            then 'auditavel'
            when metodologia_custo_vida is null then 'legado_nao_auditavel'
            else 'incompleto'
        end as qualidade_custo_vida,
        case
            when fonte_salario = 'declarado'
             and coalesce(salario_declarado_consistente, false)
             and coalesce(geo_confidence, 0) >= 0.8
             and coalesce(geo_mapping_method, '') in ('exact', 'satellite')
             and metodologia_custo_vida = 'official_sources_v2'
            then 'alta'
            when confianca_salario = 'alta'
             and coalesce(geo_confidence, 0) >= 0.7
             and coalesce(geo_mapping_method, '') in ('exact', 'satellite')
             and metodologia_custo_vida = 'official_sources_v2'
            then 'media'
            else 'baixa'
        end as qualidade_ivf,
        case
            when salario_bruto_anual is null then 'Sem Dados Salariais'
            when poder_compra_real_mensal is null or custo_total_mensal is null then 'Sem Dados Salariais'
            when (poder_compra_real_mensal - custo_total_mensal) > 1500 then 'Prosperidade'
            when (poder_compra_real_mensal - custo_total_mensal) >= 0 then 'Equilíbrio'
            else 'Risco Financeiro'
        end as classificacao_viabilidade_bruta
    from poder
)

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
    noc_code as codigo_profissao,
    noc_title as nome_profissao,
    seniority as senioridade_estimada,
    noc_confidence as confianca_profissao,
    noc_mapping_method as metodo_classificacao_profissao,
    noc_cache_key as chave_classificacao_profissao,
    noc_evidence as evidencia_profissao,
    noc_model as modelo_classificacao_profissao,
    noc_prompt_version as versao_prompt_profissao,
    noc_classified_at_utc as classificado_em_utc,
    noc_has_wage_benchmark as profissao_tem_referencia_salarial,
    cma_padronizada as regiao_metropolitana,
    geo_mapping_method as metodo_localizacao,
    geo_confidence as confianca_localizacao,
    salario_declarado as salario_declarado_na_vaga,
    salario_declarado_maximo as salario_declarado_maximo_na_vaga,
    salario_adzuna_predito,
    salario_declarado_consistente,
    salario_bruto_anual_arred as salario_bruto_anual_estimado,
    true as eh_estimativa,
    salario_estimado as salario_foi_estimado,
    familia_cargo as familia_profissional,
    case fonte_salario
        when 'declarado' then 'declarado_na_vaga'
        when 'pesquisa_vaga_verificada' then 'estimado_pesquisa_vaga'
        when 'pesquisa_empresa_verificada' then 'estimado_pesquisa_empresa'
        when 'pesquisa_web_verificada' then 'estimado_pesquisa_web'
        when 'adzuna_predito' then 'estimado_adzuna'
        when 'oficial_noc_provincia' then 'estimado_governo_provincia'
        when 'oficial_noc_nacional' then 'estimado_governo_nacional'
        when 'mercado_cargo_provincia' then 'estimado_mercado_cargo_provincia'
        when 'mercado_empresa' then 'estimado_mercado_empresa'
        when 'mercado_cargo_nacional' then 'estimado_mercado_cargo_nacional'
        when 'mercado_provincia' then 'estimado_mercado_provincia'
        when 'mercado_nacional' then 'estimado_mercado_nacional'
        else 'indisponivel'
    end as origem_salario,
    confianca_salario as confianca_salario_estimada,
    motivo_salario_estimado,
    tamanho_amostra_salario,
    aviso_salario,
    salary_research_found as pesquisa_salarial_encontrou,
    salary_research_annual_mid as salario_pesquisa_anual_estimado,
    salary_research_source_type as tipo_fonte_pesquisa_salarial,
    salary_research_source_url as url_fonte_pesquisa_salarial,
    salary_research_confidence as confianca_pesquisa_salarial,
    salary_research_evidence as evidencia_pesquisa_salarial,
    salary_research_rejection_reason as motivo_rejeicao_pesquisa_salarial,
    salario_oficial_minimo as salario_referencia_governo_minimo,
    salario_oficial_mediano as salario_referencia_governo_mediano,
    salario_oficial_maximo as salario_referencia_governo_maximo,
    salario_oficial_p95 as salario_referencia_governo_p95,
    salario_oficial_outlier as salario_referencia_governo_atipico,
    motivo_outlier_salario as motivo_salario_referencia_atipico,
    fonte_salario_oficial as origem_referencia_governo,
    ano_referencia_salario_oficial as ano_referencia_governo,
    abrangencia_salario_oficial as abrangencia_referencia_governo,
    nome_cidade as cidade,
    sigla_provincia,
    nome_provincia,
    round(
        (imposto_federal_estimado + imposto_provincial_estimado)
        / nullif(salario_bruto_anual, 0),
        4
    ) as aliquota_imposto_renda_estimada,
    round(
        (
            imposto_federal_estimado
            + imposto_provincial_estimado
            + contribuicao_cpp_qpp
            + premio_ei
            + premio_qpip
        ) / nullif(salario_bruto_anual, 0),
        4
    ) as aliquota_deducoes_estimadas,
    round(imposto_federal_estimado, 0) as imposto_federal_anual_estimado,
    round(imposto_provincial_estimado, 0) as imposto_provincial_anual_estimado,
    round(credito_federal_estimado, 0) as credito_federal_estimado,
    round(credito_provincial_estimado, 0) as credito_provincial_estimado,
    valor_pessoal_basico_provincial,
    round(contribuicao_cpp_qpp, 0) as contribuicao_cpp_qpp_anual_estimada,
    round(premio_ei, 0) as premio_ei_anual_estimado,
    round(premio_qpip, 0) as premio_qpip_anual_estimado,
    salario_liquido_anual_arred as salario_liquido_anual_estimado,
    poder_compra_real_mensal_arred as poder_compra_mensal_estimado,
    round(aluguel_medio_1bdr, 0) as aluguel_1quarto_estimado,
    round(custo_vida_sem_aluguel, 0) as custo_sem_aluguel_estimado,
    fonte_moradia_url,
    fonte_custo_vida_url,
    ano_fonte_moradia,
    ano_fonte_custo_vida,
    qualidade_fonte_moradia,
    metodo_custo_vida,
    custo_vida_estimado,
    custo_base_provincial_2023,
    fator_domicilio_unipessoal,
    fator_cpi_2026,
    cpi_mes_referencia,
    metodologia_custo_vida,
    qualidade_custo_vida,
    custo_total_mensal_arred as custo_total_mensal_estimado,
    'cra_progressivo_2026_v2' as modelo_fiscal,
    'Tudo neste calculo e ESTIMATIVA. Usa faixas fiscais de 2026 (CRA/Revenu Quebec), CPP/QPP, EI e QPIP, mas nao cobre todos os creditos, sobretaxas ou situacoes pessoais. Nao e aconselhamento fiscal, financeiro ou migratorio.' as aviso_modelo_fiscal,
    'CRA T4032 2026; Revenu Quebec 2026' as fonte_modelo_fiscal,
    qualidade_ivf as confianca_calculo,
    (
        qualidade_ivf = 'alta'
        and fonte_salario = 'declarado'
        and coalesce(salario_declarado_consistente, false)
    ) as ranking_confiavel,
    ivf_score_arred as sobra_mensal_estimada,
    classificacao_viabilidade_bruta,
    case
        when classificacao_viabilidade_bruta = 'Sem Dados Salariais'
            then 'Estimativa indisponivel — sem dados salariais'
        else 'Estimativa — ' || classificacao_viabilidade_bruta
    end as classificacao_viabilidade_estimada
from qualidade
