with historico as (
    select
        vaga_id,
        titulo_cargo,
        empresa,
        categoria_adzuna,
        descricao_vaga,
        noc_code,
        noc_title,
        seniority,
        noc_confidence,
        noc_mapping_method,
        localizacao_bruta,
        cidade_padronizada,
        provincia_padronizada,
        cma_padronizada,
        geo_mapping_method,
        geo_confidence,
        url_vaga,
        data_criacao,
        data_snapshot,
        {{ dbt_utils.generate_surrogate_key([
            'titulo_cargo',
            'empresa',
            'categoria_adzuna',
            'descricao_vaga',
            'noc_code',
            'seniority',
            'localizacao_bruta',
            'cidade_padronizada',
            'provincia_padronizada',
            'cma_padronizada',
            'url_vaga'
        ]) }} as hash_versao
    from {{ ref('stg_adzuna_jobs') }}
    where vaga_id is not null
),

mudancas as (
    select
        *,
        case
            when lag(hash_versao) over (
                partition by vaga_id order by data_snapshot
            ) is distinct from hash_versao then 1
            else 0
        end as inicio_nova_versao
    from historico
),

grupos as (
    select
        *,
        sum(inicio_nova_versao) over (
            partition by vaga_id
            order by data_snapshot
            rows between unbounded preceding and current row
        ) as numero_versao
    from mudancas
),

versoes as (
    select
        vaga_id,
        numero_versao,
        arg_max(titulo_cargo, data_snapshot) as titulo_cargo,
        arg_max(empresa, data_snapshot) as empresa,
        arg_max(categoria_adzuna, data_snapshot) as categoria_adzuna,
        arg_max(descricao_vaga, data_snapshot) as descricao_vaga,
        arg_max(noc_code, data_snapshot) as noc_code,
        arg_max(noc_title, data_snapshot) as noc_title,
        arg_max(seniority, data_snapshot) as seniority,
        arg_max(noc_confidence, data_snapshot) as noc_confidence,
        arg_max(noc_mapping_method, data_snapshot) as noc_mapping_method,
        arg_max(localizacao_bruta, data_snapshot) as localizacao_bruta,
        arg_max(cidade_padronizada, data_snapshot) as cidade_padronizada,
        arg_max(provincia_padronizada, data_snapshot) as provincia_padronizada,
        arg_max(cma_padronizada, data_snapshot) as cma_padronizada,
        arg_max(geo_mapping_method, data_snapshot) as geo_mapping_method,
        arg_max(geo_confidence, data_snapshot) as geo_confidence,
        arg_max(url_vaga, data_snapshot) as url_vaga,
        min(data_criacao) as data_criacao,
        min(data_snapshot) as valid_from,
        max(data_snapshot) as last_seen_at
    from grupos
    group by vaga_id, numero_versao
),

intervalos as (
    select
        *,
        lead(valid_from) over (
            partition by vaga_id order by numero_versao
        ) as valid_to
    from versoes
)

select
    {{ dbt_utils.generate_surrogate_key(['vaga_id', 'valid_from']) }} as sk_vaga,
    vaga_id,
    numero_versao,
    titulo_cargo,
    empresa,
    categoria_adzuna,
    descricao_vaga,
    noc_code,
    noc_title,
    seniority,
    noc_confidence,
    noc_mapping_method,
    localizacao_bruta,
    cidade_padronizada,
    provincia_padronizada,
    cma_padronizada,
    geo_mapping_method,
    geo_confidence,
    url_vaga,
    data_criacao,
    valid_from,
    valid_to,
    last_seen_at,
    valid_to is null as is_current
from intervalos
