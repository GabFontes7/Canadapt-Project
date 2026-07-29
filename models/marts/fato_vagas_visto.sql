with jobs as (
    select
        vaga_id,
        titulo_cargo,
        salario_bruto_anual,
        salario_bruto_anual_maximo,
        salario_adzuna_estimado,
        empresa,
        noc_code,
        noc_title,
        seniority,
        noc_confidence,
        cma_padronizada,
        geo_mapping_method,
        geo_confidence,
        url_vaga,
        data_criacao,
        extracted_at_utc,
        pipeline_run_id,
        data_snapshot,
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
    salario_bruto_anual,
    salario_bruto_anual_maximo,
    salario_adzuna_estimado,
    empresa,
    noc_code,
    noc_title,
    seniority,
    noc_confidence,
    cma_padronizada,
    geo_mapping_method,
    geo_confidence,
    url_vaga,
    data_criacao,
    extracted_at_utc,
    pipeline_run_id,
    first_seen_at,
    last_seen_at,
    true as is_current
from jobs
-- Produto consome apenas vagas observadas no snapshot mais recente.
where rn = 1
  and last_seen_at = data_snapshot_atual
