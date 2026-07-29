# CanAdapt

Pipeline de dados semanal para comparar vagas canadenses com salários oficiais,
custos regionais e uma estimativa transparente de viabilidade financeira.

## Arquitetura

```text
Adzuna + ESDC + Gemini
        |
        v
Bronze JSON/CSV (local + S3, imutável)
        |
        v
Silver Parquet (local + S3, histórico por partição)
        |
        v
dbt + DuckDB
        |
        +--> Gold Parquet (fonte canônica local + S3)
        `--> DuckDB local (exploração e build)
```

O pipeline coleta os últimos sete dias semanalmente e **acrescenta** a nova
partição ao histórico. O modelo `fct_vagas_snapshot` mantém as observações por
data; `dim_vaga` mantém as versões SCD2 (`valid_from`, `valid_to`, `is_current`).

## Execução local

Requer Python 3.11 e um `.env` baseado em `.env.example`.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
dbt deps --profiles-dir .
python scripts\run_pipeline.py
```

O orquestrador executa ingestões, Silver, enriquecimento NOC, `dbt run`,
`dbt test`, publicação Gold e validação. Cada execução recebe um `run_id` e
grava um manifesto em `data/metadata/runs/`.

## Automação

`.github/workflows/weekly_pipeline.yml` roda toda segunda-feira às 12:00 UTC e
também permite execução manual. Antes da carga, o runner restaura o histórico
Silver e os caches do S3.

Configure no GitHub Actions:

- `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`
- `GEMINI_API_KEY`
- `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
- `AWS_DEFAULT_REGION`
- `AWS_BUCKET_NAME`, `AWS_S3_BUCKET_NAME`

## Gold

- Canônico: `data/gold/parquet/<tabela>/year=.../month=.../day=.../`
- Atalho atual: `data/gold/parquet/<tabela>/latest/`
- Exploração local: `data/gold/canadapt_analytics.duckdb`

O link direto para candidatura está em
`fct_viabilidade_vagas.url_vaga`.

Para abrir o DuckDB no DBeaver, use conexão read-only. Desconecte o DBeaver
antes de executar dbt, pois uma conexão de escrita mantém lock no arquivo.

## Salários

A cascata é:

1. salário válido declarado na vaga;
2. mediana oficial ESDC por NOC e província;
3. mediana oficial ESDC por NOC nacional;
4. benchmarks internos identificados em `fonte_salario`.

Valores estimados incluem `aviso_salario`, `confianca_salario`, NOC, fonte e
ano de referência. O IVF usa `modelo_fiscal = simplificado_v1`; ele é uma
estimativa de produto, não aconselhamento fiscal.
