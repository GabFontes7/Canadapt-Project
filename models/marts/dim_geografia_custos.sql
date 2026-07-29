with source as (
    select distinct
        nome_cidade,
        sigla_provincia,
        nome_provincia,
        aluguel_medio_1bdr,
        custo_vida_sem_aluguel,
        aliquota_gst,
        aliquota_pst,
        aliquota_hst_total
    from {{ ref('stg_cost_of_living') }}
),

media_nacional as (
    select
        'REMOTE' as nome_cidade,
        'CANADA' as sigla_provincia,
        'Canada (Média Remoto)' as nome_provincia,
        avg(aluguel_medio_1bdr) as aluguel_medio_1bdr,
        avg(custo_vida_sem_aluguel) as custo_vida_sem_aluguel,
        0.05 as aliquota_gst,
        0.00 as aliquota_pst,
        0.05 as aliquota_hst_total
    from source
),

unificado as (
    select * from source
    union all
    select * from media_nacional
)

select
    {{ dbt_utils.generate_surrogate_key(['nome_cidade', 'sigla_provincia']) }} as sk_geografia,
    nome_cidade,
    sigla_provincia,
    nome_provincia,
    aluguel_medio_1bdr,
    custo_vida_sem_aluguel,
    aliquota_gst,
    aliquota_pst,
    aliquota_hst_total
from unificado
