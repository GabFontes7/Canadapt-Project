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
    make_date(year::integer, month::integer, day::integer) as data_snapshot
from source
