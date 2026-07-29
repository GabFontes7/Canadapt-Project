"""
CanAdapt — Quick Gold validation against the official dbt DuckDB warehouse.
"""

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
        print("=" * 72)
        print("1) Linha sintética REMOTE — dim_geografia_custos")
        print("=" * 72)
        remote_rows = con.execute(
            """
            SELECT
                sk_geografia,
                nome_cidade,
                sigla_provincia,
                nome_provincia,
                aluguel_medio_1bdr,
                custo_vida_sem_aluguel,
                (aluguel_medio_1bdr + custo_vida_sem_aluguel) AS custo_total_mensal_medio,
                aliquota_gst,
                aliquota_pst,
                aliquota_hst_total
            FROM main.dim_geografia_custos
            WHERE upper(nome_cidade) = 'REMOTE'
               OR upper(sigla_provincia) = 'CANADA'
            """
        ).fetchall()

        if not remote_rows:
            print("Nenhuma linha REMOTE/CANADA encontrada na dimensão.")
        else:
            cols = [
                "sk_geografia",
                "nome_cidade",
                "sigla_provincia",
                "nome_provincia",
                "aluguel_medio_1bdr",
                "custo_vida_sem_aluguel",
                "custo_total_mensal_medio",
                "aliquota_gst",
                "aliquota_pst",
                "aliquota_hst_total",
            ]
            for row in remote_rows:
                for col, val in zip(cols, row):
                    print(f"  {col}: {val}")
                print()

        print("=" * 72)
        print("2) Amostra (3 linhas) — fct_viabilidade_vagas")
        print("=" * 72)
        sample_rows = con.execute(
            """
            SELECT
                titulo_cargo,
                poder_compra_real_mensal,
                classificacao_viabilidade
            FROM main.fct_viabilidade_vagas
            LIMIT 3
            """
        ).fetchall()

        if not sample_rows:
            print("Tabela fct_viabilidade_vagas vazia.")
        else:
            for i, (titulo, poder_compra, classificacao) in enumerate(sample_rows, start=1):
                print(f"  [{i}] titulo_cargo: {titulo}")
                print(f"      poder_compra_real_mensal: {poder_compra}")
                print(f"      classificacao_viabilidade: {classificacao}")
                print()

    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
