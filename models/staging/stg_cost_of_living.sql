select
    sigla_provincia,
    nome_provincia,
    aliquota_gst,
    aliquota_pst,
    aliquota_hst_total,
    nome_cidade,
    aluguel_medio_1bdr,
    custo_vida_sem_aluguel
from read_parquet('data/silver/cost_of_living/year=2026/month=07/day=15/cost_of_living_clean.parquet')
