# CanAdapt

Pipeline de dados semanal para comparar vagas canadenses com salários oficiais,
custos regionais e uma estimativa transparente de viabilidade financeira.

## Arquitetura

```text
Adzuna + ESDC + CMHC + StatCan + CRA/Revenu Québec
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

## Aplicação Streamlit

```powershell
streamlit run app.py
```

O app usa `data/gold/parquet/.../latest/` e, quando os arquivos não existem
localmente, baixa os aliases `latest` do S3. O filtro padrão exibe apenas
`ranking_confiavel = true`: salário declarado consistente, localização
confiável e cálculo estimado de alta confiança.

Para abrir o DuckDB no DBeaver, use conexão read-only. Desconecte o DBeaver
antes de executar dbt, pois uma conexão de escrita mantém lock no arquivo.

## Salários

A cascata estimada e auditável é:

1. salário válido declarado na vaga;
2. pesquisa assistida por Gemini com fonte https verificável
   (mesma vaga / página da empresa / agregador reputável);
3. referência governamental ESDC por código de profissão e província;
4. referência governamental ESDC por código de profissão nacional;
5. faixa predita pela Adzuna;
6. benchmarks internos identificados em `origem_salario`.

Gemini atua somente como pesquisador: um número sem fonte rastreável é
descartado. Quando nenhuma evidência atende ao contrato, o salário cai para
a próxima fonte da cascata ou fica como `estimativa indisponível`.

Referências salariais por hora são anualizadas por 2.080 somente quando a fonte
as identifica explicitamente como horárias. Períodos ausentes ou ambíguos são
preservados para auditoria, mas não entram no cálculo. Referências
governamentais estatisticamente atípicas também são auditadas, porém ignoradas
na cascata salarial.

Todas as saídas monetárias usam nomes explícitos como
`salario_bruto_anual_estimado` e `sobra_mensal_estimada`. Mesmo quando o salário
foi declarado na vaga ou a referência veio do governo, o resultado do produto
continua sendo uma estimativa, não uma garantia ou aconselhamento.

Aluguéis são extraídos diretamente do CMHC Rental Market Survey 2025. O custo
sem aluguel deriva das tabelas StatCan SHS 2023 por província, ajustadas para
domicílio unipessoal e pelo CPI de 2026. O JSON Bronze registra URLs, anos,
fatores e mês de referência; essa ingestão não usa LLM.

O NOC usa cache v2 por fingerprint de título + categoria + descrição. Entradas
com baixa confiança ou versão antiga do prompt são reclassificadas.
