{% set partition = var('silver_cost_of_living_partition', var('silver_partition')) %}

with source as (
    select *
    from read_parquet(
        '{{ var("silver_cost_of_living_root") }}/{{ partition }}/cost_of_living_clean.parquet',
        hive_partitioning = true
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
    custo_vida_sem_aluguel
from source
cross join latest_partition
where make_date(year::integer, month::integer, day::integer) = partition_date
