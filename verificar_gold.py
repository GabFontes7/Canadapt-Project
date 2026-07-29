"""CanAdapt — validation summary for the current and historical Gold layer."""

from __future__ import annotations

from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parent
DB_PATH = PROJECT_ROOT / "data" / "gold" / "canadapt_analytics.duckdb"


def main() -> int:
    if not DB_PATH.exists():
        print(f"ERRO: banco Gold não encontrado em {DB_PATH}")
        print("Rode antes: dbt run --profiles-dir .")
        return 1

    print(f"Conectando em: {DB_PATH}\n")
    con = duckdb.connect(str(DB_PATH), read_only=True)

    try:
        sections = {
            "1) Cobertura temporal": """
                select
                    (select count(*) from main.fct_viabilidade_vagas) as vagas_atuais,
                    (select count(*) from main.fct_vagas_snapshot) as observacoes_historicas,
                    (select count(*) from main.dim_vaga where is_current) as dimensao_atual,
                    (select count(*) from main.dim_vaga where not is_current) as vagas_fechadas,
                    (select count(distinct data_snapshot) from main.fct_vagas_snapshot) as snapshots
            """,
            "2) Qualidade e cobertura": """
                select
                    count(*) filter (where url_vaga is not null) as com_link,
                    count(*) filter (where noc_code is not null) as com_noc,
                    count(*) filter (where extracted_at_utc is not null) as com_lineage,
                    count(*) filter (where elegivel_ranking) as elegiveis_ranking,
                    count(*) filter (where salario_oficial_outlier) as salarios_outlier,
                    count(*) filter (
                        where geo_mapping_method in ('country_generic', 'remote')
                    ) as geo_generica
                from main.fct_viabilidade_vagas
            """,
        }
        for title, sql in sections.items():
            print("=" * 72)
            print(title)
            print("=" * 72)
            frame = con.execute(sql).fetchdf()
            print(frame.to_string(index=False))
            print()

        print("=" * 72)
        print("3) Fontes salariais e qualidade IVF")
        print("=" * 72)
        print(
            con.execute(
                """
                select fonte_salario, confianca_salario, qualidade_ivf,
                       elegivel_ranking, count(*) as vagas
                from main.fct_viabilidade_vagas
                group by 1, 2, 3, 4
                order by vagas desc
                """
            ).fetchdf().to_string(index=False)
        )
        print()

        print("=" * 72)
        print("4) Top 5 do ranking seguro")
        print("=" * 72)
        print(
            con.execute(
                """
                select titulo_cargo, empresa, nome_cidade, sigla_provincia,
                       salario_bruto_anual, ivf_score, url_vaga
                from main.fct_viabilidade_vagas
                where elegivel_ranking
                order by ivf_score desc
                limit 5
                """
            ).fetchdf().to_string(index=False)
        )

    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
