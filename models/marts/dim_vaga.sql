with historico as (
    select
        vaga_id,
        fonte_vaga,
        id_vaga_na_fonte,
        site_origem,
        titulo_cargo,
        empresa,
        categoria_adzuna,
        area_foco_coleta,
        sinais_mobilidade,
        versao_filtro_coleta,
        tipo_contrato,
        salario_texto_original,
        descricao_vaga,
        salario_bruto_anual,
        salario_bruto_anual_maximo,
        salario_adzuna_estimado,
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
        localizacao_bruta,
        cidade_padronizada,
        provincia_padronizada,
        cma_padronizada,
        geo_mapping_method,
        geo_confidence,
        url_vaga,
        data_criacao,
        extracted_at_utc,
        pipeline_run_id,
        data_snapshot,
        {{ dbt_utils.generate_surrogate_key([
            'fonte_vaga',
            'id_vaga_na_fonte',
            'titulo_cargo',
            'empresa',
            'categoria_adzuna',
            'descricao_vaga',
            'salario_bruto_anual',
            'salario_bruto_anual_maximo',
            'salario_adzuna_estimado',
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

snapshot_atual as (
    select max(data_snapshot) as data_snapshot_atual
    from historico
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
        arg_max(fonte_vaga, data_snapshot) as fonte_vaga,
        arg_max(id_vaga_na_fonte, data_snapshot) as id_vaga_na_fonte,
        arg_max(site_origem, data_snapshot) as site_origem,
        arg_max(titulo_cargo, data_snapshot) as titulo_cargo,
        arg_max(empresa, data_snapshot) as empresa,
        arg_max(categoria_adzuna, data_snapshot) as categoria_adzuna,
        arg_max(area_foco_coleta, data_snapshot) as area_foco_coleta,
        arg_max(sinais_mobilidade, data_snapshot) as sinais_mobilidade,
        arg_max(versao_filtro_coleta, data_snapshot) as versao_filtro_coleta,
        arg_max(tipo_contrato, data_snapshot) as tipo_contrato,
        arg_max(salario_texto_original, data_snapshot) as salario_texto_original,
        arg_max(descricao_vaga, data_snapshot) as descricao_vaga,
        arg_max(salario_bruto_anual, data_snapshot) as salario_bruto_anual,
        arg_max(salario_bruto_anual_maximo, data_snapshot) as salario_bruto_anual_maximo,
        arg_max(salario_adzuna_estimado, data_snapshot) as salario_adzuna_estimado,
        arg_max(noc_code, data_snapshot) as noc_code,
        arg_max(noc_title, data_snapshot) as noc_title,
        arg_max(seniority, data_snapshot) as seniority,
        arg_max(noc_confidence, data_snapshot) as noc_confidence,
        arg_max(noc_mapping_method, data_snapshot) as noc_mapping_method,
        arg_max(noc_cache_key, data_snapshot) as noc_cache_key,
        arg_max(noc_evidence, data_snapshot) as noc_evidence,
        arg_max(noc_model, data_snapshot) as noc_model,
        arg_max(noc_prompt_version, data_snapshot) as noc_prompt_version,
        arg_max(noc_classified_at_utc, data_snapshot) as noc_classified_at_utc,
        arg_max(noc_has_wage_benchmark, data_snapshot) as noc_has_wage_benchmark,
        arg_max(localizacao_bruta, data_snapshot) as localizacao_bruta,
        arg_max(cidade_padronizada, data_snapshot) as cidade_padronizada,
        arg_max(provincia_padronizada, data_snapshot) as provincia_padronizada,
        arg_max(cma_padronizada, data_snapshot) as cma_padronizada,
        arg_max(geo_mapping_method, data_snapshot) as geo_mapping_method,
        arg_max(geo_confidence, data_snapshot) as geo_confidence,
        arg_max(url_vaga, data_snapshot) as url_vaga,
        arg_max(extracted_at_utc, data_snapshot) as extracted_at_utc,
        arg_max(pipeline_run_id, data_snapshot) as pipeline_run_id,
        min(data_criacao) as data_criacao,
        min(data_snapshot) as valid_from,
        max(data_snapshot) as last_seen_at
    from grupos
    group by vaga_id, numero_versao
),

intervalos as (
    select
        v.*,
        lead(v.valid_from) over (
            partition by v.vaga_id order by v.numero_versao
        ) as valid_to_por_versao,
        s.data_snapshot_atual
    from versoes v
    cross join snapshot_atual s
),

fechado as (
    select
        *,
        case
            when valid_to_por_versao is not null then valid_to_por_versao
            -- Ausente no snapshot mais recente: fecha a vigência na data atual.
            when last_seen_at < data_snapshot_atual then data_snapshot_atual
            else null
        end as valid_to
    from intervalos
)

select
    {{ dbt_utils.generate_surrogate_key(['vaga_id', 'valid_from']) }} as sk_vaga,
    vaga_id,
    numero_versao,
    fonte_vaga,
    id_vaga_na_fonte,
    site_origem,
    titulo_cargo,
    empresa,
    categoria_adzuna,
    area_foco_coleta,
    sinais_mobilidade,
    versao_filtro_coleta,
    tipo_contrato,
    salario_texto_original,
    descricao_vaga,
    salario_bruto_anual,
    salario_bruto_anual_maximo,
    salario_adzuna_estimado,
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
    localizacao_bruta,
    cidade_padronizada,
    provincia_padronizada,
    cma_padronizada,
    geo_mapping_method,
    geo_confidence,
    url_vaga,
    data_criacao,
    extracted_at_utc,
    pipeline_run_id,
    valid_from,
    valid_to,
    last_seen_at,
    data_snapshot_atual,
    (valid_to is null and last_seen_at = data_snapshot_atual) as is_current
from fechado
