with jobs as (
    select
        vaga_id,
        titulo_cargo,
        descricao_vaga,
        salario_bruto_anual,
        salario_bruto_anual_maximo,
        salario_adzuna_estimado,
        empresa,
        noc_code,
        noc_title,
        seniority,
        noc_confidence,
        noc_mapping_method,
        noc_cache_key,
        noc_evidence,
        noc_model,
        noc_prompt_version,
        noc_classified_at_utc,
        noc_has_wage_benchmark,
        cma_padronizada,
        geo_mapping_method,
        geo_confidence,
        url_vaga,
        data_criacao,
        extracted_at_utc,
        pipeline_run_id,
        data_snapshot,
        salary_research_found,
        salary_research_annual_min,
        salary_research_annual_mid,
        salary_research_annual_max,
        salary_research_period_original,
        salary_research_currency,
        salary_research_source_type,
        salary_research_source_url,
        salary_research_source_title,
        salary_research_observed_date,
        salary_research_confidence,
        salary_research_evidence,
        salary_research_rejection_reason,
        salary_research_model,
        salary_research_prompt_version,
        salary_research_at_utc,
        salary_research_cache_key,
        min(data_snapshot) over (partition by vaga_id) as first_seen_at,
        max(data_snapshot) over (partition by vaga_id) as last_seen_at,
        max(data_snapshot) over () as data_snapshot_atual,
        row_number() over (
            partition by vaga_id
            order by data_snapshot desc, data_criacao desc nulls last
        ) as rn,
        case
            when lower(cma_padronizada) = 'remote' then 'REMOTE'
            else cma_padronizada
        end as cidade_chave,
        case
            when lower(cma_padronizada) = 'remote'
              or lower(provincia_padronizada) = 'remote'
            then 'CANADA'
            else provincia_padronizada
        end as provincia_chave
    from {{ ref('stg_adzuna_jobs') }}
    where vaga_id is not null
)

select
    vaga_id,
    {{ dbt_utils.generate_surrogate_key(['cidade_chave', 'provincia_chave']) }} as sk_geografia,
    titulo_cargo,
    descricao_vaga,
    salario_bruto_anual,
    salario_bruto_anual_maximo,
    salario_adzuna_estimado,
    empresa,
    noc_code,
    noc_title,
    seniority,
    noc_confidence,
    noc_mapping_method,
    noc_cache_key,
    noc_evidence,
    noc_model,
    noc_prompt_version,
    noc_classified_at_utc,
    noc_has_wage_benchmark,
    cma_padronizada,
    geo_mapping_method,
    geo_confidence,
    url_vaga,
    data_criacao,
    extracted_at_utc,
    pipeline_run_id,
    salary_research_found,
    salary_research_annual_min,
    salary_research_annual_mid,
    salary_research_annual_max,
    salary_research_period_original,
    salary_research_currency,
    salary_research_source_type,
    salary_research_source_url,
    salary_research_source_title,
    salary_research_observed_date,
    salary_research_confidence,
    salary_research_evidence,
    salary_research_rejection_reason,
    salary_research_model,
    salary_research_prompt_version,
    salary_research_at_utc,
    salary_research_cache_key,
    first_seen_at,
    last_seen_at,
    true as is_current
from jobs
-- Produto consome apenas vagas observadas no snapshot mais recente.
where rn = 1
  and last_seen_at = data_snapshot_atual
