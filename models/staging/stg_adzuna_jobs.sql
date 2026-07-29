{% set partition = var('silver_jobs_partition', var('silver_partition')) %}

with source as (
    select *
    from read_parquet(
        '{{ var("silver_jobs_root") }}/{{ partition }}/jobs_clean.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
)

select
    job_id as vaga_id,
    title as titulo_cargo,
    company as empresa,
    salary_min as salario_bruto_anual,
    salary_max as salario_bruto_anual_maximo,
    salary_is_predicted as salario_adzuna_estimado,
    category as categoria_adzuna,
    description as descricao_vaga,
    noc_code,
    noc_title,
    seniority,
    noc_confidence,
    noc_mapping_method,
    noc_cache_key,
    noc_evidence,
    noc_model,
    noc_prompt_version,
    try_cast(noc_classified_at_utc as timestamp) as noc_classified_at_utc,
    noc_has_wage_benchmark,
    location_raw as localizacao_bruta,
    cidade_padronizada,
    provincia_padronizada,
    cma_padronizada,
    geo_mapping_method,
    geo_confidence,
    redirect_url as url_vaga,
    cast(created as timestamp) as data_criacao,
    try_cast(extracted_at_utc as timestamp) as extracted_at_utc,
    coalesce(pipeline_run_id, 'unknown') as pipeline_run_id,
    make_date(year::integer, month::integer, day::integer) as data_snapshot,
    coalesce(salary_research_found, false) as salary_research_found,
    try_cast(salary_research_annual_min as double) as salary_research_annual_min,
    try_cast(salary_research_annual_mid as double) as salary_research_annual_mid,
    try_cast(salary_research_annual_max as double) as salary_research_annual_max,
    cast(salary_research_period_original as varchar) as salary_research_period_original,
    cast(salary_research_currency as varchar) as salary_research_currency,
    cast(salary_research_source_type as varchar) as salary_research_source_type,
    cast(salary_research_source_url as varchar) as salary_research_source_url,
    cast(salary_research_source_title as varchar) as salary_research_source_title,
    cast(salary_research_observed_date as varchar) as salary_research_observed_date,
    try_cast(salary_research_confidence as double) as salary_research_confidence,
    cast(salary_research_evidence as varchar) as salary_research_evidence,
    cast(salary_research_rejection_reason as varchar) as salary_research_rejection_reason,
    cast(salary_research_model as varchar) as salary_research_model,
    cast(salary_research_prompt_version as varchar) as salary_research_prompt_version,
    try_cast(salary_research_at_utc as timestamp) as salary_research_at_utc,
    cast(salary_research_cache_key as varchar) as salary_research_cache_key
from source
