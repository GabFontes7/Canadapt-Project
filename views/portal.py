"""CanAdapt — portal de vagas no Canadá sobre a camada Gold canônica."""

from __future__ import annotations

import os
import sys
from pathlib import Path

import boto3
import duckdb
import pandas as pd
import streamlit as st

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT / "src" / "ingestion") not in sys.path:
    sys.path.insert(0, str(ROOT / "src" / "ingestion"))
from mobility_filter import (
    mobility_confirmed,
    mobility_signal_labels,
    parse_mobility_signals,
)

GOLD_ROOT = ROOT / "data" / "gold" / "parquet"
FCT_PATH = GOLD_ROOT / "fct_viabilidade_vagas" / "latest" / "fct_viabilidade_vagas.parquet"
REMOTE_PATH = (
    GOLD_ROOT / "cenarios_vaga_remota" / "latest" / "cenarios_vaga_remota.parquet"
)
SNAPSHOT_PATH = GOLD_ROOT / "fct_vagas_snapshot" / "latest" / "fct_vagas_snapshot.parquet"

VAGAS_POR_PAGINA = 8

AREA_LABELS = {
    "tecnologia_engenharia": "Tecnologia e engenharia",
    "gestao_consultoria": "Gestão e consultoria",
    "vendas_marketing": "Vendas e marketing",
    "operacoes_logistica": "Operações e logística",
    "produto_projetos": "Produto e projetos",
    "financas_conformidade": "Finanças e conformidade",
    "financas_bancario": "Bancos, risco e operações financeiras",
    "educacao": "Educação",
    "saude": "Saúde",
    "outros": "Outras áreas",
}

SENIORIDADE_LABELS = {
    "entry": "Início de carreira",
    "junior": "Júnior",
    "mid": "Pleno",
    "senior": "Sênior",
    "lead": "Líder técnico",
    "manager": "Gerência",
    "director": "Diretoria",
    "executive": "Executivo",
    "unknown": "Senioridade não identificada",
}

ORIGEM_SALARIO_LABELS = {
    "declarado_na_vaga": "Salário declarado no anúncio",
    "estimado_pesquisa_vaga": "Estimado por pesquisa no próprio anúncio",
    "estimado_pesquisa_empresa": "Estimado por pesquisa na empresa",
    "estimado_pesquisa_web": "Estimado por pesquisa pública com fonte",
    "estimado_adzuna": "Estimado pelo modelo da Adzuna",
    "estimado_governo_provincia": "Estimado por dados do governo na província",
    "estimado_governo_nacional": "Estimado por dados do governo no Canadá",
    "estimado_mercado_cargo_provincia": "Estimado pela mediana da área na província",
    "estimado_mercado_cargo_nacional": "Estimado pela mediana da área no Canadá",
    "estimado_mercado_empresa": "Estimado pela mediana da empresa",
    "estimado_mercado_provincia": "Estimado pela mediana da província",
    "estimado_mercado_nacional": "Estimado pela mediana do Canadá",
    "indisponivel": "Sem base salarial disponível",
}

CONFIANCA_LABELS = {
    "alta": ("Confiança alta", "green"),
    "media": ("Confiança média", "orange"),
    "baixa": ("Confiança baixa", "gray"),
}

ORDENACOES = {
    "Maior sobra mensal": ("sobra_mensal_estimada", False),
    "Maior salário": ("salario_bruto_anual_estimado", False),
    "Publicadas primeiro": ("data_criacao", False),
}


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
        jobs = con.execute("select * from read_parquet(?)", [str(FCT_PATH)]).fetchdf()
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


def rotulo_area(valor: object) -> str:
    if valor is None or pd.isna(valor):
        return "Área não classificada"
    chave = str(valor)
    return AREA_LABELS.get(chave, chave.replace("_", " ").capitalize())


def rotulo_senioridade(valor: object) -> str | None:
    if valor is None or pd.isna(valor):
        return None
    return SENIORIDADE_LABELS.get(str(valor), str(valor).capitalize())


def rotulo_origem_salario(valor: object) -> str:
    if valor is None or pd.isna(valor):
        return "Origem do salário não informada"
    return ORIGEM_SALARIO_LABELS.get(str(valor), str(valor).replace("_", " "))


def cad(valor: object) -> str:
    if valor is None or pd.isna(valor):
        return "—"
    return "CAD " + f"{float(valor):,.0f}".replace(",", ".")


def selo_viabilidade(valor: object) -> tuple[str, str]:
    texto = "" if valor is None or pd.isna(valor) else str(valor)
    if "Prosperidade" in texto:
        return "Sobra confortável", "green"
    if "Equil" in texto:
        return "No limite do equilíbrio", "orange"
    if "Risco" in texto:
        return "Risco financeiro", "red"
    return "Sem base salarial", "gray"


def selo_confianca(valor: object) -> tuple[str, str]:
    chave = "" if valor is None or pd.isna(valor) else str(valor)
    return CONFIANCA_LABELS.get(chave, ("Confiança não avaliada", "gray"))


def local_da_vaga(row: pd.Series) -> str:
    cidade = row.get("cidade")
    provincia = row.get("sigla_provincia")
    if pd.isna(cidade) or not str(cidade).strip():
        return "Local não informado"
    if pd.isna(provincia) or not str(provincia).strip():
        return str(cidade).title()
    return f"{str(cidade).title()}, {provincia}"


def mobilidade_confirmada(row: pd.Series) -> bool:
    return mobility_confirmed(row.get("sinais_mobilidade"))


def rotulos_mobilidade(row: pd.Series) -> list[str]:
    return mobility_signal_labels(row.get("sinais_mobilidade"))


def badge_mobilidade(row: pd.Series) -> tuple[str, str] | None:
    keys = set(parse_mobility_signals(row.get("sinais_mobilidade")))
    if not keys:
        return None
    if "lmia_approved" in keys:
        return "LMIA aprovado (Job Bank)", "green"
    if "lmia_requested" in keys or (
        "lmia" in keys and str(row.get("fonte_vaga") or "").casefold() == "jobbank"
    ):
        return "LMIA solicitado (Job Bank)", "green"
    if "international_candidates" in keys:
        return "Aberto a internacionais (Job Bank)", "green"
    rotulos = rotulos_mobilidade(row)
    rotulo = (
        "Mobilidade confirmada"
        if len(rotulos) <= 1
        else "Mobilidade confirmada: " + ", ".join(rotulos)
    )
    return rotulo, "green"


def apenas_busca_api(row: pd.Series) -> bool:
    fonte = str(row.get("fonte_vaga") or "").casefold()
    versao = str(row.get("versao_filtro_coleta") or "")
    return fonte == "adzuna" and not mobilidade_confirmada(row) and (
        not versao or versao.startswith("adzuna_mobility_queries")
    )


def render_vaga(row: pd.Series, *, contexto_remoto: str | None = None) -> None:
    titulo = str(row.get("titulo_cargo") or "Vaga sem título")
    empresa = str(row.get("empresa") or "Empresa não informada")
    viab_texto, viab_cor = selo_viabilidade(row.get("classificacao_viabilidade_estimada"))
    conf_texto, conf_cor = selo_confianca(row.get("confianca_calculo"))
    senioridade = rotulo_senioridade(row.get("senioridade_estimada"))
    url = row.get("url_vaga")
    descricao = row.get("descricao_vaga")

    with st.container(border=True):
        info, numeros = st.columns([3, 1.35], vertical_alignment="top")

        with info:
            st.markdown(f"##### {titulo}")
            local = contexto_remoto or local_da_vaga(row)
            fonte = row.get("fonte_vaga")
            site_origem = row.get("site_origem")
            origem = ""
            if pd.notna(site_origem) and str(site_origem).strip():
                origem = f"  ·  via {str(site_origem).strip()}"
            elif pd.notna(fonte) and str(fonte).strip():
                origem = f"  ·  via {str(fonte).strip().title()}"
            st.caption(f"{empresa}  ·  {local}{origem}")

            with st.container(horizontal=True, gap="small"):
                st.badge(viab_texto, color=viab_cor)
                if row.get("confianca_calculo") is not None:
                    st.badge(conf_texto, color=conf_cor)
                if mobilidade_confirmada(row):
                    badge_texto, badge_cor = badge_mobilidade(row) or (
                        "Mobilidade confirmada",
                        "green",
                    )
                    st.badge(badge_texto, color=badge_cor, icon=":material/verified_user:")
                st.badge(rotulo_area(row.get("familia_profissional")), color="gray")
                if senioridade:
                    st.badge(senioridade, color="gray")
                if bool(row.get("ranking_confiavel")) or bool(
                    row.get("ranking_confiavel_remoto")
                ):
                    st.badge(
                        "Entra no ranking",
                        color="primary",
                        icon=":material/verified:",
                    )

            st.caption(rotulo_origem_salario(row.get("origem_salario")))

            if apenas_busca_api(row):
                st.warning(
                    "Esta vaga entrou só pela busca na API, sem menção explícita a "
                    "patrocínio, LMIA ou relocação no texto do anúncio.",
                    icon=":material/info:",
                )

        with numeros:
            st.metric(
                "Salário anual estimado",
                cad(row.get("salario_bruto_anual_estimado")),
                help="Valor bruto por ano. Pode vir do anúncio ou de estimativa com fonte.",
            )
            st.caption(
                f"Sobra mensal estimada {cad(row.get('sobra_mensal_estimada'))}  ·  "
                f"custo/mês {cad(row.get('custo_total_mensal_estimado'))}"
            )
            if isinstance(url, str) and url.startswith("http"):
                st.link_button(
                    "Ver vaga",
                    url,
                    type="primary",
                    icon=":material/open_in_new:",
                    width="stretch",
                )

        with st.expander("Descrição da vaga", icon=":material/description:"):
            if isinstance(descricao, str) and descricao.strip():
                st.write(descricao.strip())
            else:
                st.caption("A fonte não disponibilizou uma descrição para esta vaga.")

        with st.expander("Detalhes da estimativa", icon=":material/query_stats:"):
            det1, det2, det3 = st.columns(3)
            det1.markdown(
                f"**Salário bruto/ano**  \n{cad(row.get('salario_bruto_anual_estimado'))}"
            )
            det1.markdown(
                f"**Salário líquido/ano**  \n{cad(row.get('salario_liquido_anual_estimado'))}"
            )
            det2.markdown(
                f"**Aluguel 1 quarto/mês**  \n{cad(row.get('aluguel_1quarto_estimado'))}"
            )
            det2.markdown(
                f"**Outros custos/mês**  \n{cad(row.get('custo_sem_aluguel_estimado'))}"
            )
            profissao = row.get("nome_profissao")
            codigo = row.get("codigo_profissao")
            if pd.notna(profissao):
                det3.markdown(f"**Profissão**  \n{profissao} ({codigo})")
            det3.markdown(
                f"**Base do salário**  \n{rotulo_origem_salario(row.get('origem_salario'))}"
            )

            fontes = []
            for coluna, rotulo in [
                ("url_fonte_pesquisa_salarial", "Fonte do salário"),
                ("fonte_moradia_url", "Fonte de moradia"),
                ("fonte_custo_vida_url", "Fonte de custo de vida"),
            ]:
                valor = row.get(coluna)
                if isinstance(valor, str) and valor.startswith("http"):
                    fontes.append(f"[{rotulo}]({valor})")
            if fontes:
                st.caption("Fontes: " + "  ·  ".join(fontes))

            aviso = row.get("aviso_salario")
            if isinstance(aviso, str) and aviso.strip():
                st.caption(aviso.lstrip("* ").strip())


def render() -> None:
    marca, resumo = st.columns([2.4, 1], vertical_alignment="center")
    with marca:
        st.title("Explorar vagas")
        st.caption(
            "Vagas no Canadá com apoio a visto — e uma estimativa de quanto sobra no fim do mês."
        )

    try:
        jobs, snapshots, remotas = load_data()
    except Exception as exc:  # noqa: BLE001
        st.error(f"Não foi possível carregar a Gold: {exc}", icon=":material/error:")
        st.stop()

    is_remote_job = (
        jobs["metodo_localizacao"].isin(["country_generic", "remote"])
        | (jobs["cidade"].astype(str).str.upper() == "REMOTE")
    )
    localized = jobs[~is_remote_job].copy()
    remote_jobs_base = jobs[is_remote_job].copy()

    ultima_coleta = pd.to_datetime(jobs["extracted_at_utc"], errors="coerce").max()
    with resumo:
        st.caption(
            f"**{len(jobs)}** vagas na base  ·  **{len(localized)}** com cidade definida"
            + (
                f"  ·  atualizado em {ultima_coleta:%d/%m/%Y}"
                if pd.notna(ultima_coleta)
                else ""
            )
        )

    with st.sidebar:
        st.subheader("Sobre o CanAdapt")
        st.caption(
            "Cruzamos vagas canadenses do Job Bank (LMIA / internacionais) e de "
            "agregadores que mencionam patrocínio ou relocação com impostos de 2026 "
            "e custo de vida por cidade."
        )
        st.warning(
            "Todos os valores são estimativas. Não é aconselhamento financeiro, "
            "fiscal ou migratório.",
            icon=":material/warning:",
        )
        st.caption("Fontes: Job Bank · Adzuna · Jooble · ESDC · StatCan · CMHC")

    with st.container(border=True):
        busca = st.text_input(
            "Buscar vaga",
            placeholder="Cargo, empresa ou palavra-chave (ex.: data analyst, Shopify)",
            icon=":material/search:",
            label_visibility="collapsed",
        )
        f1, f2, f3 = st.columns([1.2, 1.2, 1.6])
        areas_disponiveis = sorted(jobs["familia_profissional"].dropna().unique().tolist())
        areas = f1.multiselect(
            "Área profissional",
            options=areas_disponiveis,
            format_func=rotulo_area,
            placeholder="Todas as áreas",
        )
        provincias = f2.multiselect(
            "Província",
            options=sorted(localized["sigla_provincia"].dropna().unique().tolist()),
            placeholder="Todas as províncias",
        )
        salario_minimo = f3.slider(
            "Salário anual mínimo estimado (CAD)",
            min_value=0,
            max_value=200_000,
            value=0,
            step=10_000,
            format="CAD %d",
        )
        with st.container(horizontal=True, gap="medium"):
            somente_mobilidade = st.toggle(
                "Somente mobilidade confirmada",
                value=True,
                help=(
                    "Mostra vagas cujo anúncio menciona patrocínio de visto, LMIA, "
                    "relocação ou apoio à imigração."
                ),
            )
            ranking_seguro = st.toggle(
                "Somente vagas de ranking confiável",
                value=True,
                help="Exige salário declarado no anúncio e localização de alta confiança.",
            )
            incluir_estimativas = st.toggle(
                "Incluir salários estimados por outras fontes",
                value=False,
                disabled=ranking_seguro,
            )


    def aplicar_busca(df: pd.DataFrame) -> pd.DataFrame:
        if not busca.strip():
            return df
        termo = busca.strip().lower()
        alvo = (
            df["titulo_cargo"].fillna("").str.lower()
            + " "
            + df["empresa"].fillna("").str.lower()
            + " "
            + df.get("nome_profissao", pd.Series("", index=df.index)).fillna("").str.lower()
            + " "
            + df.get("descricao_vaga", pd.Series("", index=df.index)).fillna("").str.lower()
        )
        return df[alvo.str.contains(termo, regex=False)]


    def aplicar_filtros_comuns(df: pd.DataFrame, *, coluna_ranking: str) -> pd.DataFrame:
        out = df.copy()
        if somente_mobilidade and "sinais_mobilidade" in out.columns:
            out = out[out.apply(mobilidade_confirmada, axis=1)]
        if ranking_seguro and coluna_ranking in out.columns:
            out = out[out[coluna_ranking] == True]  # noqa: E712
        elif not incluir_estimativas:
            out = out[out["salario_foi_estimado"] == False]  # noqa: E712
        if areas:
            out = out[out["familia_profissional"].isin(areas)]
        out = out[out["salario_bruto_anual_estimado"].fillna(0) >= salario_minimo]
        return aplicar_busca(out)


    def ordenar(df: pd.DataFrame, criterio: str) -> pd.DataFrame:
        coluna, ascending = ORDENACOES[criterio]
        if coluna not in df.columns:
            return df
        return df.sort_values(coluna, ascending=ascending, na_position="last")


    def render_lista(df: pd.DataFrame, *, chave: str, contexto_remoto: str | None = None) -> None:
        if df.empty:
            st.info(
                "Nenhuma vaga com esses filtros. Tente ampliar a busca, desligar "
                "'Somente mobilidade confirmada' ou o ranking confiável.",
                icon=":material/search_off:",
            )
            return

        total_paginas = max(1, (len(df) + VAGAS_POR_PAGINA - 1) // VAGAS_POR_PAGINA)
        lista = st.container()
        if total_paginas > 1:
            with st.container(horizontal=True, horizontal_alignment="center"):
                pagina = st.pagination(total_paginas, key=f"pag_{chave}_{total_paginas}")
        else:
            pagina = 1

        inicio = (pagina - 1) * VAGAS_POR_PAGINA
        with lista:
            for _, row in df.iloc[inicio : inicio + VAGAS_POR_PAGINA].iterrows():
                render_vaga(row, contexto_remoto=contexto_remoto)


    aba_local, aba_remota, aba_metodo = st.tabs(
        [
            "Vagas por cidade",
            "Vagas remotas",
            "Como calculamos",
        ]
    )

    with aba_local:
        filtradas = aplicar_filtros_comuns(localized, coluna_ranking="ranking_confiavel")
        if provincias:
            filtradas = filtradas[filtradas["sigla_provincia"].isin(provincias)]

        with st.container(horizontal=True, vertical_alignment="center"):
            ordem_local = st.segmented_control(
                "Ordenar por",
                options=list(ORDENACOES),
                default="Maior sobra mensal",
                label_visibility="collapsed",
                key="ordem_local",
            )
            st.caption(f"{len(filtradas)} vagas encontradas")

        filtradas = ordenar(filtradas, ordem_local or "Maior sobra mensal")
        render_lista(filtradas, chave="local")

        with st.expander("Ver como tabela", icon=":material/table_chart:"):
            colunas = [
                c
                for c in [
                    "titulo_cargo",
                    "empresa",
                    "cidade",
                    "sigla_provincia",
                    "salario_bruto_anual_estimado",
                    "origem_salario",
                    "confianca_calculo",
                    "sobra_mensal_estimada",
                    "classificacao_viabilidade_estimada",
                    "url_vaga",
                ]
                if c in filtradas.columns
            ]
            st.dataframe(
                filtradas[colunas],
                width="stretch",
                hide_index=True,
                column_config={
                    "titulo_cargo": "Vaga",
                    "empresa": "Empresa",
                    "cidade": "Cidade",
                    "sigla_provincia": "Província",
                    "salario_bruto_anual_estimado": st.column_config.NumberColumn(
                        "Salário anual estimado", format="$ %.0f"
                    ),
                    "origem_salario": "Origem do salário",
                    "confianca_calculo": "Confiança do cálculo",
                    "sobra_mensal_estimada": st.column_config.NumberColumn(
                        "Sobra mensal estimada", format="$ %.0f"
                    ),
                    "classificacao_viabilidade_estimada": "Classificação estimada",
                    "url_vaga": st.column_config.LinkColumn(
                        "Candidatura", display_text="Abrir vaga"
                    ),
                },
            )

    with aba_remota:
        if remotas.empty:
            st.info(
                f"Ainda não há cenários remotos publicados ({len(remote_jobs_base)} vagas "
                "remotas na base). Rode o pipeline para gerar os cenários.",
                icon=":material/public:",
            )
        else:
            st.caption(
                "A vaga continua remota. Escolha onde você moraria para ver imposto e "
                "custo de vida daquela cidade."
            )
            cidades = sorted(
                remotas.loc[remotas["tipo_cenario"] == "cidade_ancora", "cidade_cenario"]
                .dropna()
                .unique()
                .tolist()
            )
            escolha_cidade = st.selectbox(
                "Cidade onde eu moraria",
                options=cidades,
                index=cidades.index("Calgary") if "Calgary" in cidades else 0,
            )

            cenario = remotas[
                (remotas["tipo_cenario"] == "cidade_ancora")
                & (remotas["cidade_cenario"] == escolha_cidade)
            ].copy()
            cenario = aplicar_filtros_comuns(
                cenario, coluna_ranking="ranking_confiavel_remoto"
            )

            with st.container(horizontal=True, vertical_alignment="center"):
                ordem_remota = st.segmented_control(
                    "Ordenar por",
                    options=list(ORDENACOES),
                    default="Maior sobra mensal",
                    label_visibility="collapsed",
                    key="ordem_remota",
                )
                st.caption(f"{len(cenario)} vagas remotas neste cenário")

            cenario = ordenar(cenario, ordem_remota or "Maior sobra mensal")
            provincia_cenario = (
                cenario["provincia_cenario"].dropna().iloc[0] if not cenario.empty else ""
            )
            render_lista(
                cenario,
                chave="remota",
                contexto_remoto=f"Remoto  ·  cenário {escolha_cidade}, {provincia_cenario}",
            )

            with st.expander(
                "Melhores cidades para uma vaga remota específica",
                icon=":material/travel_explore:",
            ):
                labels = (
                    cenario.assign(
                        label=lambda d: d["titulo_cargo"].fillna("?")
                        + " — "
                        + d["empresa"].fillna("?")
                    )[["vaga_id", "label"]]
                    .drop_duplicates("vaga_id")
                    .sort_values("label")
                )
                if labels.empty:
                    st.caption("Nenhuma vaga remota nos filtros atuais.")
                else:
                    vaga_escolhida = st.selectbox(
                        "Vaga remota",
                        options=labels["vaga_id"].tolist(),
                        format_func=lambda vid: labels.loc[
                            labels["vaga_id"] == vid, "label"
                        ].iloc[0],
                    )
                    por_vaga = remotas[
                        (remotas["vaga_id"] == vaga_escolhida)
                        & (remotas["tipo_cenario"] == "cidade_ancora")
                    ].sort_values(
                        "sobra_mensal_estimada", ascending=False, na_position="last"
                    )
                    st.dataframe(
                        por_vaga[
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
                            "ranking_cidade_na_vaga": "Posição",
                        },
                    )

    with aba_metodo:
        st.subheader("Como o CanAdapt estima a sobra mensal")
        st.markdown(
            """
            1. Coletamos vagas canadenses que citam LMIA, patrocínio ou apoio a relocação.
            2. Classificamos a profissão e a área a partir do título e da descrição.
            3. Definimos o salário nesta ordem: declarado no anúncio → pesquisa pública com
               fonte → dados do governo → modelo da Adzuna → mediana de mercado.
            4. Aplicamos as faixas de imposto de 2026 (federal e provincial) para chegar ao
               líquido estimado.
            5. Subtraímos aluguel e custo de vida da cidade para chegar à sobra mensal estimada.
            """
        )
        st.warning(
            "Nada aqui é valor oficial garantido. O cálculo usa faixas fiscais de 2026 e "
            "custos de referência (CMHC/StatCan), mas não cobre sua situação pessoal.",
            icon=":material/warning:",
        )

        q1, q2, q3 = st.columns(3)
        q1.metric("Vagas na base", len(jobs), border=True)
        q2.metric(
            "Com salário declarado",
            int((jobs["origem_salario"] == "declarado_na_vaga").sum()),
            border=True,
        )
        q3.metric(
            "No ranking confiável",
            int(jobs["ranking_confiavel"].fillna(False).sum()),
            border=True,
        )

        st.subheader("Histórico de coletas")
        if snapshots.empty:
            st.caption("Nenhum snapshot disponível.")
        else:
            st.bar_chart(snapshots.set_index("data_snapshot")["vagas"], height=220)

        st.subheader("Glossário")
        st.markdown(
            """
            - **Sobra mensal estimada** — salário líquido estimado menos custo de vida estimado.
            - **Ranking confiável** — só entra com salário declarado consistente e cidade identificada.
            - **Confiança do cálculo** — quão sólido está o cenário (alta, média ou baixa).
            - **Área profissional** — agrupamento do cargo (tecnologia, saúde, operações...).
            - **Profissão** — classificação ocupacional canadense do cargo.
            - **Vagas remotas** — a vaga segue remota; você escolhe a cidade do cenário.
            """
        )
