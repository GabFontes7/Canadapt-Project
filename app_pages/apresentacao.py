"""Página inicial de apresentação do CanAdapt."""

from __future__ import annotations

import pandas as pd
import streamlit as st

from views.portal import load_data


def _kpis() -> tuple[int | None, int | None, str | None]:
    try:
        jobs, _snapshots, _remotas = load_data()
    except Exception:  # noqa: BLE001
        return None, None, None
    ranking = (
        int(jobs["ranking_confiavel"].fillna(False).sum())
        if "ranking_confiavel" in jobs.columns
        else 0
    )
    coleta = pd.to_datetime(jobs.get("extracted_at_utc"), errors="coerce").max()
    quando = coleta.strftime("%d/%m/%Y") if pd.notna(coleta) else None
    return len(jobs), ranking, quando


st.badge("Portal de decisão · Canadá", color="red", icon=":material/public:")
st.title("CanAdapt")
st.markdown(
    "Vagas canadenses com sinal de **mobilidade** — visto, LMIA ou relocação — "
    "e uma estimativa transparente do que pode **sobrar no fim do mês**."
)

vagas, ranking, atualizado = _kpis()
k1, k2, k3 = st.columns(3)
k1.metric("Vagas na base atual", "—" if vagas is None else vagas, border=True)
k2.metric(
    "No ranking confiável",
    "—" if ranking is None else ranking,
    border=True,
    help="Só entram anúncios com salário declarado e localização confiável.",
)
k3.metric("Última coleta", atualizado or "—", border=True)

st.page_link(
    "app_pages/vagas.py",
    label="Explorar vagas",
    icon=":material/work:",
    width="stretch",
)

st.subheader("Para quem é")
st.write(
    "O CanAdapt ajuda quem avalia uma oferta ou uma busca de trabalho no "
    "Canadá e precisa cruzar o anúncio com imposto, aluguel e custo de vida "
    "da cidade — sem tratar o número como garantia oficial."
)

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/flight: Mobilidade**")
        st.caption(
            "A coleta prioriza vagas que citam LMIA, patrocínio de visto "
            "ou apoio a relocação internacional."
        )
with c2:
    with st.container(border=True):
        st.markdown("**:material/payments: Sobra mensal estimada**")
        st.caption(
            "Do bruto ao líquido (faixas CRA 2026) e depois aluguel e "
            "custo de vida CMHC/StatCan da cidade."
        )
with c3:
    with st.container(border=True):
        st.markdown("**:material/verified: Fontes à vista**")
        st.caption(
            "Cada card mostra de onde veio o salário e links das bases "
            "oficiais usadas no cálculo."
        )

st.subheader("Como os dados chegam até aqui")
b1, b2, b3 = st.columns(3)
with b1:
    with st.container(border=True):
        st.markdown("**Bronze**")
        st.caption(
            "JSON/CSV bruto da semana, local e no S3, sem reescrever o passado."
        )
with b2:
    with st.container(border=True):
        st.markdown("**Silver**")
        st.caption(
            "Cidade padronizada, classificação NOC, salário pesquisado com fonte e caches."
        )
with b3:
    with st.container(border=True):
        st.markdown("**Gold**")
        st.caption(
            "dbt + DuckDB: viabilidade, ranking confiável e cenários remotos."
        )

st.subheader("O que você encontra no portal")
st.markdown(
    """
    - **Vagas por cidade** — ranking pela sobra mensal estimada, com filtros de área e província.
    - **Vagas remotas** — a vaga continua remota; você escolhe a cidade do cenário de custo.
    - **Como calculamos** — ordem das fontes de salário, impostos e o glossário dos selos.
    """
)

st.subheader("Fontes")
with st.container(horizontal=True, gap="small"):
    st.badge("Adzuna", color="red")
    st.badge("Jooble", color="red")
    st.badge("ESDC / NOC", color="blue")
    st.badge("StatCan", color="blue")
    st.badge("CMHC", color="blue")
    st.badge("CRA 2026", color="green")

st.warning(
    "Todos os valores são estimativas. O CanAdapt não substitui aconselhamento "
    "financeiro, fiscal ou migratório, nem garante que a vaga patrocine o seu visto.",
    icon=":material/warning:",
)

with st.sidebar:
    st.subheader("CanAdapt")
    st.caption(
        "Portal acadêmico de decisão para vagas no Canadá, com pipeline "
        "semanal Bronze → Silver → Gold."
    )
    st.caption("Tema: vermelho Canadá · dados oficiais · estimativa explícita.")
