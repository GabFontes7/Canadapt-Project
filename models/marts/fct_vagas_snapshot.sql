with snapshots as (
    select
        *,
        row_number() over (
            partition by vaga_id, data_snapshot
            order by data_criacao desc nulls last
        ) as rn
    from {{ ref('stg_adzuna_jobs') }}
    where vaga_id is not null
),

normalizado as (
    select
        vaga_id,
        data_snapshot,
        salario_bruto_anual,
        salario_bruto_anual_maximo,
        salario_adzuna_estimado,
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
    from snapshots
    where rn = 1
)

select
    {{ dbt_utils.generate_surrogate_key(['n.vaga_id', 'n.data_snapshot']) }} as sk_snapshot,
    d.sk_vaga,
    n.vaga_id,
    {{ dbt_utils.generate_surrogate_key(['n.cidade_chave', 'n.provincia_chave']) }} as sk_geografia,
    n.data_snapshot,
    n.salario_bruto_anual,
    n.salario_bruto_anual_maximo,
    n.salario_adzuna_estimado
from normalizado n
left join {{ ref('dim_vaga') }} d
    on n.vaga_id = d.vaga_id
   and n.data_snapshot >= d.valid_from
   and (d.valid_to is null or n.data_snapshot < d.valid_to)
