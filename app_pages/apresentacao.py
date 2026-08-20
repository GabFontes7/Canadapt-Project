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


st.badge("Vagas no Canadá com mobilidade", color="red", icon=":material/public:")
hero, convite = st.columns([2.2, 1], vertical_alignment="center")
with hero:
    st.title("🍁 CanAdapt")
    st.markdown(
        "Encontre vagas canadenses abertas a quem precisa de **visto, LMIA ou relocação** "
        "— e veja uma estimativa do que pode **sobrar no fim do mês** naquela cidade."
    )
with convite:
    with st.container(border=True):
        st.caption("Comece por aqui")
        if st.button(
            "Ver vagas no Canadá",
            type="primary",
            icon=":material/work:",
            width="stretch",
        ):
            st.switch_page("app_pages/vagas.py")
        st.caption("Filtre por cidade, área e salário estimado.")

vagas, ranking, atualizado = _kpis()
k1, k2, k3 = st.columns(3)
k1.metric("Vagas na base atual", "—" if vagas is None else vagas, border=True)
k2.metric(
    "No ranking confiável",
    "—" if ranking is None else ranking,
    border=True,
    help="Só entram anúncios com salário declarado e localização confiável.",
)
k3.metric("Última atualização", atualizado or "—", border=True)

st.subheader("Para quem é")
st.write(
    "Para quem está pesquisando trabalho no Canadá e quer ir além do anúncio: "
    "saber se a vaga realmente sinaliza mobilidade e quanto pode sobrar depois de "
    "imposto, aluguel e custo de vida da cidade."
)

c1, c2, c3 = st.columns(3)
with c1:
    with st.container(border=True):
        st.markdown("**:material/flight: Mobilidade em evidência**")
        st.caption(
            "Priorizamos Job Bank (LMIA / internacionais) e anúncios que citam "
            "patrocínio de visto ou relocação."
        )
with c2:
    with st.container(border=True):
        st.markdown("**:material/payments: Sobra mensal estimada**")
        st.caption(
            "Do bruto ao líquido (faixas de 2026) e depois aluguel e custo de vida "
            "da cidade (CMHC / StatCan)."
        )
with c3:
    with st.container(border=True):
        st.markdown("**:material/verified: Fontes à vista**")
        st.caption(
            "Cada vaga mostra de onde veio o salário e links das bases usadas "
            "no cálculo."
        )

st.subheader("O que você encontra")
st.markdown(
    """
    - **Vagas por cidade** — ordene pela sobra mensal estimada e filtre por área ou província.
    - **Vagas remotas** — a vaga segue remota; você escolhe a cidade só para o cenário de custo.
    - **Como calculamos** — de onde vem o salário, como entra o imposto e o que cada selo significa.
    """
)

st.subheader("Fontes")
with st.container(horizontal=True, gap="small"):
    st.badge("Job Bank", color="red")
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
