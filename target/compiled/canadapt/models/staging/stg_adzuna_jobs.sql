select
    job_id as vaga_id,
    title as titulo_cargo,
    company as empresa,
    salary_min as salario_bruto_anual,
    cidade_padronizada,
    provincia_padronizada,
    redirect_url as url_vaga,
    cast(created as timestamp) as data_criacao
from read_parquet('data/silver/jobs/year=2026/month=07/day=15/jobs_clean.parquet')