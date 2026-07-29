{% set partition = var('silver_cost_of_living_partition', var('silver_partition')) %}

with source as (
    select *
    from read_parquet(
        '{{ var("silver_cost_of_living_root") }}/{{ partition }}/cost_of_living_clean.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
),

latest_partition as (
    select max(make_date(year::integer, month::integer, day::integer)) as partition_date
    from source
)

select
    sigla_provincia,
    nome_provincia,
    aliquota_gst,
    aliquota_pst,
    aliquota_hst_total,
    nome_cidade,
    aluguel_medio_1bdr,
    custo_vida_sem_aluguel,
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
from source
cross join latest_partition
where make_date(year::integer, month::integer, day::integer) = partition_date
