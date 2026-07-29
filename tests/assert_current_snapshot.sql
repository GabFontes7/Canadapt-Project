-- Current product fact must only contain jobs from the latest observed snapshot.
with latest as (
    select max(data_snapshot) as latest_snapshot
    from {{ ref('fct_vagas_snapshot') }}
),

expected as (
    select count(distinct vaga_id) as jobs
    from {{ ref('fct_vagas_snapshot') }}
    cross join latest
    where data_snapshot = latest_snapshot
),

actual as (
    select count(*) as jobs
    from {{ ref('fct_viabilidade_vagas') }}
)

select actual.jobs, expected.jobs
from actual cross join expected
where actual.jobs <> expected.jobs
