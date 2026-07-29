with source as (
    select distinct
        nome_cidade,
        sigla_provincia,
        nome_provincia,
        aluguel_medio_1bdr,
        custo_vida_sem_aluguel,
        aliquota_gst,
        aliquota_pst,
        aliquota_hst_total,
        fonte_moradia_verificada,
        fonte_custo_vida_verificada,
        fonte_moradia_url,
        fonte_custo_vida_url,
        ano_fonte_moradia,
        ano_fonte_custo_vida,
        qualidade_fonte_moradia,
        metodo_custo_vida,
        custo_vida_estimado,
        custo_base_provincial_2023,
        fator_domicilio_unipessoal,
        fator_cpi_2026,
        cpi_mes_referencia,
        fonte_imposto_url,
        vigencia_imposto,
        consultado_em_utc,
        metodologia_versao
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
        0.05 as aliquota_hst_total,
        'CMHC Rental Market Survey' as fonte_moradia_verificada,
        'Statistics Canada SHS + CPI' as fonte_custo_vida_verificada,
        null::varchar as fonte_moradia_url,
        null::varchar as fonte_custo_vida_url,
        max(ano_fonte_moradia) as ano_fonte_moradia,
        max(ano_fonte_custo_vida) as ano_fonte_custo_vida,
        'unknown' as qualidade_fonte_moradia,
        'national_average' as metodo_custo_vida,
        true as custo_vida_estimado,
        avg(custo_base_provincial_2023) as custo_base_provincial_2023,
        avg(fator_domicilio_unipessoal) as fator_domicilio_unipessoal,
        avg(fator_cpi_2026) as fator_cpi_2026,
        max(cpi_mes_referencia) as cpi_mes_referencia,
        null::varchar as fonte_imposto_url,
        null::varchar as vigencia_imposto,
        max(consultado_em_utc) as consultado_em_utc,
        max(metodologia_versao) as metodologia_versao
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
    nome_cidade as cma_padronizada,
    sigla_provincia,
    nome_provincia,
    aluguel_medio_1bdr,
    custo_vida_sem_aluguel,
    aliquota_gst,
    aliquota_pst,
    aliquota_hst_total,
    fonte_moradia_verificada,
    fonte_custo_vida_verificada,
    fonte_moradia_url,
    fonte_custo_vida_url,
    ano_fonte_moradia,
    ano_fonte_custo_vida,
    qualidade_fonte_moradia,
    metodo_custo_vida,
    custo_vida_estimado,
    custo_base_provincial_2023,
    fator_domicilio_unipessoal,
    fator_cpi_2026,
    cpi_mes_referencia,
    fonte_imposto_url,
    vigencia_imposto,
    consultado_em_utc,
    metodologia_versao
from unificado
