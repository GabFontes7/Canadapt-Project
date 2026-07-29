with source as (
    select *
    from read_parquet(
        '{{ var("silver_wages_root") }}/reference_year={{ var("wages_reference_year") }}/wages_official.parquet',
        hive_partitioning = true,
        union_by_name = true
    )
),

latest_reference as (
    select max(reference_year::integer) as reference_year
    from source
)

select
    noc_code,
    noc_title,
    province_code,
    economic_region_code,
    economic_region_name,
    wage_period_flag_raw,
    wage_period,
    wage_is_annual,
    annualization_factor,
    salary_annual_low,
    salary_annual_median,
    salary_annual_high,
    source_reference_period,
    source_revision_date,
    dataset_reference_year,
    salary_source
from source
cross join latest_reference
where source.reference_year::integer = latest_reference.reference_year
