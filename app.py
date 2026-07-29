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
REMOTE_PATH = (
    GOLD_ROOT / "cenarios_vaga_remota" / "latest" / "cenarios_vaga_remota.parquet"
)
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
def load_data() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    _download_gold_if_needed(FCT_PATH, "fct_viabilidade_vagas")
    _download_gold_if_needed(SNAPSHOT_PATH, "fct_vagas_snapshot")
    has_remote = REMOTE_PATH.exists()
    if not has_remote:
        try:
            _download_gold_if_needed(REMOTE_PATH, "cenarios_vaga_remota")
            has_remote = REMOTE_PATH.exists()
        except Exception:  # noqa: BLE001
            has_remote = False

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
        if has_remote:
            remotas = con.execute(
                "select * from read_parquet(?)", [str(REMOTE_PATH)]
            ).fetchdf()
        else:
            remotas = pd.DataFrame()
    return jobs, snapshots, remotas


st.set_page_config(
    page_title="CanAdapt",
    page_icon="🍁",
    layout="wide",
)
st.title("🍁 CanAdapt")
st.caption(
    "Vagas no Canadá com salários e sobra mensal ESTIMADOS — "
    "nada aqui é valor oficial garantido."
)

try:
    jobs, snapshots, remotas = load_data()
except Exception as exc:  # noqa: BLE001
    st.error(f"Não foi possível carregar a Gold: {exc}")
    st.stop()

is_remote_job = (
    jobs["metodo_localizacao"].isin(["country_generic", "remote"])
    | (jobs["cidade"].astype(str).str.upper() == "REMOTE")
)
localized = jobs[~is_remote_job].copy()
remote_jobs_base = jobs[is_remote_job].copy()

with st.sidebar:
    st.header("Filtros")
    ranking_seguro = st.toggle(
        "Somente ranking confiável",
        value=True,
        help="Exige salário declarado na vaga e localização de alta confiança. Ainda assim, tudo é estimado.",
    )
    incluir_estimativas = st.toggle(
        "Incluir outras estimativas de salário",
        value=False,
        disabled=ranking_seguro,
    )
    provincias = st.multiselect(
        "Província",
        sorted(localized["sigla_provincia"].dropna().unique().tolist()),
    )
    familias = st.multiselect(
        "Família profissional",
        sorted(jobs["familia_profissional"].dropna().unique().tolist()),
    )
    salario_minimo = st.number_input(
        "Salário anual mínimo estimado (CAD)",
        min_value=0,
        value=0,
        step=5000,
    )

filtered = localized.copy()
if ranking_seguro:
    filtered = filtered[filtered["ranking_confiavel"] == True]  # noqa: E712
elif not incluir_estimativas:
    filtered = filtered[filtered["salario_foi_estimado"] == False]  # noqa: E712
if provincias:
    filtered = filtered[filtered["sigla_provincia"].isin(provincias)]
if familias:
    filtered = filtered[filtered["familia_profissional"].isin(familias)]
filtered = filtered[
    filtered["salario_bruto_anual_estimado"].fillna(0) >= salario_minimo
]
filtered = filtered.sort_values(
    "sobra_mensal_estimada", ascending=False, na_position="last"
)

st.subheader("Vagas localizadas (valores estimados)")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Vagas exibidas", f"{len(filtered):,}".replace(",", "."))
col2.metric("Vagas atuais", f"{len(localized):,}".replace(",", "."))
col3.metric(
    "Salário mediano estimado",
    f"CAD {filtered['salario_bruto_anual_estimado'].median():,.0f}"
    if not filtered.empty
    else "—",
)
col4.metric(
    "Sobra mensal mediana estimada",
    f"CAD {filtered['sobra_mensal_estimada'].median():,.0f}"
    if not filtered.empty
    else "—",
)

st.warning(
    "Tudo nesta página é ESTIMATIVA. O cálculo usa faixas fiscais de 2026 e "
    "custos de referência (CMHC/StatCan), mas não cobre sua situação pessoal. "
    "Não constitui aconselhamento financeiro, fiscal ou migratório."
)

display_columns = [
    c
    for c in [
        "titulo_cargo",
        "empresa",
        "cidade",
        "sigla_provincia",
        "salario_bruto_anual_estimado",
        "origem_salario",
        "codigo_profissao",
        "confianca_profissao",
        "confianca_calculo",
        "sobra_mensal_estimada",
        "classificacao_viabilidade_estimada",
        "url_fonte_pesquisa_salarial",
        "url_vaga",
    ]
    if c in filtered.columns
]
st.dataframe(
    filtered[display_columns],
    width="stretch",
    hide_index=True,
    column_config={
        "titulo_cargo": "Vaga",
        "empresa": "Empresa",
        "cidade": "Cidade",
        "sigla_provincia": "Província",
        "salario_bruto_anual_estimado": st.column_config.NumberColumn(
            "Salário anual estimado (CAD)", format="$ %.0f"
        ),
        "origem_salario": "Origem do salário (estimado)",
        "codigo_profissao": "Código da profissão",
        "confianca_profissao": st.column_config.NumberColumn(
            "Confiança da profissão", format="%.2f"
        ),
        "confianca_calculo": "Confiança do cálculo",
        "sobra_mensal_estimada": st.column_config.NumberColumn(
            "Sobra mensal estimada", format="$ %.0f"
        ),
        "classificacao_viabilidade_estimada": "Classificação estimada",
        "url_fonte_pesquisa_salarial": st.column_config.LinkColumn(
            "Fonte do salário ↗",
            display_text="Abrir fonte",
        ),
        "url_vaga": st.column_config.LinkColumn(
            "Candidatar-se",
            display_text="Abrir vaga ↗",
        ),
    },
)

st.divider()
st.subheader("Vagas remotas — escolha onde você moraria (estimado)")
st.caption(
    "ESTIMATIVA. Remoto no Canadá — o custo depende de onde você vai morar. "
    "Selecione uma cidade para ver a sobra mensal estimada nesse cenário."
)

if remotas.empty:
    st.info(
        f"Ainda não há cenários remotos publicados ({len(remote_jobs_base)} vagas "
        "remotas na Gold). Rode o pipeline para gerar `cenarios_vaga_remota`."
    )
else:
    cidades = (
        remotas.loc[remotas["tipo_cenario"] == "cidade_ancora", "cidade_cenario"]
        .dropna()
        .unique()
        .tolist()
    )
    cidades = sorted(cidades)
    cidade_escolhida = st.selectbox(
        "Cidade onde eu moraria",
        options=cidades,
        index=cidades.index("Calgary") if "Calgary" in cidades else 0,
        help="O salário estimado da vaga permanece o mesmo; mudam imposto e custo local estimados.",
    )

    cenario_cidade = remotas[
        (remotas["tipo_cenario"] == "cidade_ancora")
        & (remotas["cidade_cenario"] == cidade_escolhida)
    ].copy()

    if familias:
        cenario_cidade = cenario_cidade[
            cenario_cidade["familia_profissional"].isin(familias)
        ]
    cenario_cidade = cenario_cidade[
        cenario_cidade["salario_bruto_anual_estimado"].fillna(0) >= salario_minimo
    ]

    if ranking_seguro:
        cenario_cidade = cenario_cidade[
            cenario_cidade["ranking_confiavel_remoto"] == True  # noqa: E712
        ]
    elif not incluir_estimativas:
        cenario_cidade = cenario_cidade[
            cenario_cidade["salario_foi_estimado"] == False  # noqa: E712
        ]

    cenario_cidade = cenario_cidade.sort_values(
        "sobra_mensal_estimada", ascending=False, na_position="last"
    )

    ref = remotas[
        (remotas["tipo_cenario"] == "referencia_nacional")
        & (remotas["vaga_id"].isin(cenario_cidade["vaga_id"]))
    ]
    sobra_ref = ref["sobra_mensal_estimada"].median() if not ref.empty else None

    r1, r2, r3, r4 = st.columns(4)
    r1.metric("Vagas remotas no cenário", f"{len(cenario_cidade):,}".replace(",", "."))
    r2.metric(
        "Sobra mensal mediana estimada",
        f"CAD {cenario_cidade['sobra_mensal_estimada'].median():,.0f}"
        if not cenario_cidade.empty
        else "—",
    )
    r3.metric(
        "Estimativa — Prosperidade",
        int(
            (
                cenario_cidade["classificacao_viabilidade_estimada"]
                == "Estimativa — Prosperidade"
            ).sum()
        ),
    )
    r4.metric(
        "Média Canadá (referência estimada)",
        f"CAD {sobra_ref:,.0f}"
        if sobra_ref is not None and pd.notna(sobra_ref)
        else "—",
        help="Só referência — não entra no ranking.",
    )

    remote_cols = [
        "titulo_cargo",
        "empresa",
        "cidade_cenario",
        "provincia_cenario",
        "salario_bruto_anual_estimado",
        "origem_salario",
        "custo_total_mensal_estimado",
        "sobra_mensal_estimada",
        "classificacao_viabilidade_estimada",
        "url_vaga",
    ]
    st.dataframe(
        cenario_cidade[remote_cols],
        width="stretch",
        hide_index=True,
        column_config={
            "titulo_cargo": "Vaga",
            "empresa": "Empresa",
            "cidade_cenario": "Cidade do cenário",
            "provincia_cenario": "Província",
            "salario_bruto_anual_estimado": st.column_config.NumberColumn(
                "Salário anual estimado (CAD)", format="$ %.0f"
            ),
            "origem_salario": "Origem do salário (estimado)",
            "custo_total_mensal_estimado": st.column_config.NumberColumn(
                "Custo mensal estimado", format="$ %.0f"
            ),
            "sobra_mensal_estimada": st.column_config.NumberColumn(
                "Sobra mensal estimada", format="$ %.0f"
            ),
            "classificacao_viabilidade_estimada": "Classificação estimada",
            "url_vaga": st.column_config.LinkColumn(
                "Candidatar-se",
                display_text="Abrir vaga ↗",
            ),
        },
    )

    with st.expander("Melhores cidades para uma vaga remota específica (estimado)"):
        labels = (
            cenario_cidade.assign(
                label=lambda d: d["titulo_cargo"].fillna("?")
                + " — "
                + d["empresa"].fillna("?")
            )[["vaga_id", "label"]]
            .drop_duplicates("vaga_id")
            .sort_values("label")
        )
        if labels.empty:
            st.info("Nenhuma vaga remota no filtro atual.")
        else:
            escolha = st.selectbox(
                "Vaga remota",
                options=labels["vaga_id"].tolist(),
                format_func=lambda vid: labels.loc[
                    labels["vaga_id"] == vid, "label"
                ].iloc[0],
            )
            por_vaga = remotas[
                (remotas["vaga_id"] == escolha)
                & (remotas["tipo_cenario"] == "cidade_ancora")
            ].sort_values(
                "sobra_mensal_estimada", ascending=False, na_position="last"
            )

            for titulo, classe in [
                ("Melhores cidades (Estimativa — Prosperidade)", "Estimativa — Prosperidade"),
                ("Em equilíbrio (estimado)", "Estimativa — Equilíbrio"),
                ("Em risco financeiro (estimado)", "Estimativa — Risco Financeiro"),
            ]:
                bloco = por_vaga[
                    por_vaga["classificacao_viabilidade_estimada"] == classe
                ]
                st.markdown(f"**{titulo}**")
                if bloco.empty:
                    st.caption("Nenhuma neste cenário.")
                else:
                    st.dataframe(
                        bloco[
                            [
                                "cidade_cenario",
                                "provincia_cenario",
                                "custo_total_mensal_estimado",
                                "sobra_mensal_estimada",
                                "ranking_cidade_na_vaga",
                            ]
                        ],
                        hide_index=True,
                        width="stretch",
                        column_config={
                            "cidade_cenario": "Cidade",
                            "provincia_cenario": "Província",
                            "custo_total_mensal_estimado": st.column_config.NumberColumn(
                                "Custo mensal estimado", format="$ %.0f"
                            ),
                            "sobra_mensal_estimada": st.column_config.NumberColumn(
                                "Sobra mensal estimada", format="$ %.0f"
                            ),
                            "ranking_cidade_na_vaga": "Rank",
                        },
                    )

with st.expander("Histórico de coletas"):
    if snapshots.empty:
        st.info("Nenhum snapshot disponível.")
    else:
        chart = snapshots.set_index("data_snapshot")
        st.bar_chart(chart["vagas"])
        st.dataframe(snapshots, hide_index=True, width="stretch")

with st.expander("Como interpretar (glossário)"):
    st.markdown(
        """
        - **Tudo é estimado:** nenhum número é garantia oficial do que você vai ganhar ou gastar.
        - **Salário declarado na vaga:** veio do anúncio; ainda assim é estimado para o cálculo.
        - **Estimado Adzuna / governo / mercado:** fontes de referência quando a vaga não informa salário.
        - **Código da profissão:** classificação canadense da ocupação (antigo “NOC”).
        - **Sobra mensal estimada:** salário líquido estimado menos custo de vida estimado.
        - **Pesquisa com fonte:** quando a vaga não informa salário, buscamos evidência pública com URL; sem fonte, o valor é rejeitado.
        - **Confiança do cálculo:** quão sólido está o cenário (alta / média / baixa).
        - **Ranking confiável:** só entra com salário declarado consistente e localização boa.
        - **Vagas remotas:** a vaga continua remota; você escolhe a cidade e vê o cenário estimado.
        """
    )
