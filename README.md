# CanAdapt

Pipeline de dados semanal para comparar vagas canadenses com salários oficiais,
custos regionais e uma estimativa transparente de viabilidade financeira.

## Arquitetura

```text
Adzuna + Jooble + ESDC + CMHC + StatCan + CRA/Revenu Québec
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
partição ao histórico. O Silver **não** relê todo o Bronze: usa o snapshot
Adzuna/Jooble do dia mais recente e grava `silver/jobs/year=/month=/day=/`.
O modelo `fct_vagas_snapshot` mantém as observações por data; `dim_vaga`
fecha vagas que sumiram do snapshot atual (`valid_from`, `valid_to`,
`is_current`). Apagar partições Silver antigas **remove** esse histórico na
próxima rebuild do Gold — por isso a retenção padrão é 90 dias, não zero.

Cada partição Silver nova carrega `data_contract_version` e versões de
prompt (`geo_prompt_version`, `noc_prompt_version`,
`salary_research_prompt_version`). O manifesto em `data/metadata/runs/`
inclui métricas de qualidade, etapas degradadas e o resultado do quality
gate (Gold com zero vagas atuais falha o job).

Se o Gemini responder 503, as etapas de geo/NOC/salário usam cache e
seguem para o Gold com status `degraded` (não derrubam o CI).

A ingestão Adzuna roda três consultas complementares, todas exigindo sinal de
mobilidade (LMIA, patrocínio, relocação ou visto): uma multissetorial, uma na
categoria `it-jobs` ordenada por data e uma na mesma categoria ordenada por
relevância para "data". Os anúncios são deduplicados por `id` e o volume de cada
execução é limitado por `CANADAPT_MAX_JOBS_PER_RUN` (padrão 500) para conter o
custo de enriquecimento.

A ingestão Jooble é deliberadamente mais restrita: retém somente vagas de
**tecnologia** ou de **operações bancárias e financeiras** (incluindo back
office, middle office, risco, compliance, AML/KYC, tesouraria e áreas
correlatas) que também apresentem sinal explícito de mobilidade, como visa
sponsorship, LMIA/EIMT, apoio a work permit ou relocation internacional.
O filtro é aplicado novamente após a resposta da API para impedir que resultados
fora do escopo cheguem à Silver. São oito consultas por execução semanal; uma
cota inicial de 500 chamadas cobre aproximadamente 62 execuções.

## Execução local

Requer Python 3.11 e um `.env` baseado em `.env.example`.

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-pipeline.txt
dbt deps --profiles-dir .
python scripts\run_pipeline.py
```

`requirements.txt` é enxuto (só o portal Streamlit). O pipeline usa
`requirements-pipeline.txt`.

O orquestrador executa ingestões, Silver, enriquecimento NOC, `dbt run`,
`dbt test`, publicação Gold e validação. Cada execução recebe um `run_id` e
grava um manifesto em `data/metadata/runs/`.

## Automação

`.github/workflows/weekly_pipeline.yml` roda **toda quarta-feira às 12:00
Brasília** (15:00 UTC) e também permite `workflow_dispatch`.
Antes da carga, o runner restaura caches, wages e as partições hive dos
últimos `CANADAPT_LAKE_RETENTION_DAYS` dias (`scripts/restore_lake_slice.py`).

Lifecycle S3 (`infra/s3_lifecycle.json`): a AWS avalia as regras **todo dia**
e apaga objetos com mais de 90 dias em Bronze (vagas/Jooble/CoL) e Silver
(jobs/CoL). Wages, `silver/metadata` e Gold não expiram. Não é um cron de 90
dias; é idade do objeto. Aplicar no bucket:

```bash
aws s3api put-bucket-lifecycle-configuration --bucket canadapt-data-lake-gf --lifecycle-configuration file://infra/s3_lifecycle.json
```

Configure no GitHub Actions:

- `ADZUNA_APP_ID`, `ADZUNA_APP_KEY`
- `JOOBLE_API_KEY`
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
pip install -r requirements.txt
streamlit run app.py
```

O app abre na **apresentação** do projeto e tem a página **Explorar vagas**.
Usa `data/gold/parquet/.../latest/` e, quando os arquivos não existem
localmente, baixa os aliases `latest` do S3. O filtro padrão das vagas
exibe apenas `ranking_confiavel = true`: salário declarado consistente,
localização confiável e cálculo estimado de alta confiança.

### Publicar no Streamlit Community Cloud (gratuito)

Arquitetura:

```text
GitHub Actions (semanal) → escreve Gold no S3
Streamlit Community Cloud → lê Gold latest do S3
```

1. Faça push do código para o GitHub (`main`).
2. Acesse [share.streamlit.io](https://share.streamlit.io) e entre com GitHub.
3. **Create app** → repositório `Canadapt-Project` → branch `main` → arquivo `app.py`.
4. Em **Advanced settings → Secrets**, cole (modelo em
   `.streamlit/secrets.toml.example`):

```toml
AWS_ACCESS_KEY_ID = "..."
AWS_SECRET_ACCESS_KEY = "..."
AWS_DEFAULT_REGION = "us-east-1"
AWS_BUCKET_NAME = "canadapt-data-lake-gf"
AWS_S3_BUCKET_NAME = "canadapt-data-lake-gf"
```

5. Deploy. A URL fica no formato `https://<nome>.streamlit.app`.

A app **não** precisa de Adzuna nem Gemini — só leitura do S3. Idealmente a
chave AWS usada no Streamlit tem permissão apenas de leitura em
`s3://canadapt-data-lake-gf/gold/`.

Após ~12h sem visitas a app hiberna; no próximo acesso ela acorda sozinha
(pode levar alguns segundos).

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
