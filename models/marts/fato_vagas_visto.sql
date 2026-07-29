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
        min(data_snapshot) over (partition by vaga_id) as first_seen_at,
        max(data_snapshot) over (partition by vaga_id) as last_seen_at,
        row_number() over (
            partition by vaga_id
            order by data_snapshot desc, data_criacao desc nulls last
        ) as rn,
        case
            when lower(cidade_padronizada) = 'remote' then 'REMOTE'
            else cidade_padronizada
        end as cidade_chave,
        case
            when lower(cidade_padronizada) = 'remote'
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
    first_seen_at,
    last_seen_at
from jobs
where rn = 1
