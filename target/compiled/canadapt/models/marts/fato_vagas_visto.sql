with jobs as (
    select
        vaga_id,
        titulo_cargo,
        salario_bruto_anual,
        empresa,
        url_vaga,
        data_criacao,
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
    from "canadapt_analytics"."main"."stg_adzuna_jobs"
)

select
    vaga_id,
    md5(cast(coalesce(cast(cidade_chave as TEXT), '_dbt_utils_surrogate_key_null_') || '-' || coalesce(cast(provincia_chave as TEXT), '_dbt_utils_surrogate_key_null_') as TEXT)) as sk_geografia,
    titulo_cargo,
    salario_bruto_anual,
    empresa,
    url_vaga,
    data_criacao
from jobs