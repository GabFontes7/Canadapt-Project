"""CanAdapt Streamlit application over the canonical Gold Parquet layer."""

from __future__ import annotations

import os
from pathlib import Path

import boto3
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parent
GOLD_ROOT = ROOT / "data" / "gold" / "parquet"
FCT_PATH = GOLD_ROOT / "fct_viabilidade_vagas" / "latest" / "fct_viabilidade_vagas.parquet"
SNAPSHOT_PATH = GOLD_ROOT / "fct_vagas_snapshot" / "latest" / "fct_vagas_snapshot.parquet"


def _bucket() -> str:
    return (
        os.getenv("AWS_BUCKET_NAME", "").strip()
        or os.getenv("AWS_S3_BUCKET_NAME", "").strip()
    )


def _download_gold_if_needed(path: Path, table: str) -> None:
    if path.exists():
        return
    bucket = _bucket()
    if not bucket:
        raise FileNotFoundError(
            f"{path} não existe e AWS_BUCKET_NAME/AWS_S3_BUCKET_NAME não foi configurado."
        )
    path.parent.mkdir(parents=True, exist_ok=True)
    boto3.client(
        "s3",
        region_name=os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
    ).download_file(
        bucket,
        f"gold/{table}/latest/{table}.parquet",
        str(path),
    )


@st.cache_data(ttl=900)
def load_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    _download_gold_if_needed(FCT_PATH, "fct_viabilidade_vagas")
    _download_gold_if_needed(SNAPSHOT_PATH, "fct_vagas_snapshot")
    with duckdb.connect(":memory:") as con:
        jobs = con.execute(
            "select * from read_parquet(?)", [str(FCT_PATH)]
        ).fetchdf()
        snapshots = con.execute(
            """
            select data_snapshot, count(*) as vagas
            from read_parquet(?)
            group by data_snapshot
            order by data_snapshot
            """,
            [str(SNAPSHOT_PATH)],
        ).fetchdf()
    return jobs, snapshots


st.set_page_config(
    page_title="CanAdapt",
    page_icon="🍁",
    layout="wide",
)
st.title("🍁 CanAdapt")
st.caption("Vagas no Canadá, salários e viabilidade financeira estimada")

try:
    jobs, snapshots = load_data()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível carregar a Gold: {exc}")
    st.stop()

with st.sidebar:
    st.header("Filtros")
    ranking_seguro = st.toggle(
        "Somente ranking confiável",
        value=True,
        help="Exige salário declarado consistente e geografia de alta confiança.",
    )
    incluir_estimativas = st.toggle(
        "Incluir estimativas",
        value=False,
        disabled=ranking_seguro,
    )
    provincias = st.multiselect(
        "Província",
        sorted(jobs["sigla_provincia"].dropna().unique().tolist()),
    )
    familias = st.multiselect(
        "Família profissional",
        sorted(jobs["familia_cargo"].dropna().unique().tolist()),
    )
    salario_minimo = st.number_input(
        "Salário anual mínimo (CAD)",
        min_value=0,
        value=0,
        step=5000,
    )

filtered = jobs.copy()
if ranking_seguro:
    filtered = filtered[filtered["elegivel_ranking"] == True]  # noqa: E712
elif not incluir_estimativas:
    filtered = filtered[filtered["salario_estimado"] == False]  # noqa: E712
if provincias:
    filtered = filtered[filtered["sigla_provincia"].isin(provincias)]
if familias:
    filtered = filtered[filtered["familia_cargo"].isin(familias)]
filtered = filtered[filtered["salario_bruto_anual"].fillna(0) >= salario_minimo]
filtered = filtered.sort_values("ivf_score", ascending=False, na_position="last")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Vagas exibidas", f"{len(filtered):,}".replace(",", "."))
col2.metric("Vagas atuais", f"{len(jobs):,}".replace(",", "."))
col3.metric(
    "Salário mediano",
    f"CAD {filtered['salario_bruto_anual'].median():,.0f}"
    if not filtered.empty
    else "—",
)
col4.metric(
    "IVF mediano",
    f"CAD {filtered['ivf_score'].median():,.0f}"
    if not filtered.empty
    else "—",
)

st.warning(
    "O IVF usa um modelo fiscal simplificado e estimativas de custo. "
    "Não constitui aconselhamento financeiro, fiscal ou migratório."
)

display_columns = [
    "titulo_cargo",
    "empresa",
    "nome_cidade",
    "sigla_provincia",
    "salario_bruto_anual",
    "fonte_salario",
    "qualidade_ivf",
    "ivf_score",
    "classificacao_viabilidade",
    "url_vaga",
]
st.dataframe(
    filtered[display_columns],
    use_container_width=True,
    hide_index=True,
    column_config={
        "titulo_cargo": "Vaga",
        "empresa": "Empresa",
        "nome_cidade": "Cidade/CMA",
        "sigla_provincia": "Província",
        "salario_bruto_anual": st.column_config.NumberColumn(
            "Salário anual (CAD)", format="$ %.0f"
        ),
        "fonte_salario": "Fonte do salário",
        "qualidade_ivf": "Confiança IVF",
        "ivf_score": st.column_config.NumberColumn("IVF mensal", format="$ %.0f"),
        "classificacao_viabilidade": "Classificação",
        "url_vaga": st.column_config.LinkColumn(
            "Candidatar-se",
            display_text="Abrir vaga ↗",
        ),
    },
)

with st.expander("Histórico de coletas"):
    if snapshots.empty:
        st.info("Nenhum snapshot disponível.")
    else:
        chart = snapshots.set_index("data_snapshot")
        st.bar_chart(chart["vagas"])
        st.dataframe(snapshots, hide_index=True, use_container_width=True)

with st.expander("Como interpretar os dados"):
    st.markdown(
        """
        - **Salário declarado:** informado na vaga e elegível ao ranking quando consistente.
        - **Adzuna predito / ESDC-NOC:** estimativas sinalizadas; não entram no ranking seguro.
        - **Qualidade IVF alta:** salário declarado + geografia exata/satélite confiável.
        - O asterisco e os avisos de salário indicam valores não informados diretamente.
        """
    )
