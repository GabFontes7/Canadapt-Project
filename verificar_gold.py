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
        print("2) Cobertura salarial — fct_viabilidade_vagas")
        print("=" * 72)
        cobertura = con.execute(
            """
            SELECT
                count(*) AS total,
                count(*) FILTER (WHERE salario_estimado = false) AS declarados,
                count(*) FILTER (WHERE salario_estimado = true) AS estimados,
                count(*) FILTER (WHERE fonte_salario = 'mercado_cargo_provincia') AS via_cargo_provincia,
                count(*) FILTER (WHERE fonte_salario = 'mercado_empresa') AS via_empresa,
                count(*) FILTER (WHERE fonte_salario = 'mercado_cargo_nacional') AS via_cargo_nacional,
                count(*) FILTER (WHERE fonte_salario = 'mercado_provincia') AS via_provincia,
                count(*) FILTER (WHERE fonte_salario = 'mercado_nacional') AS via_nacional,
                count(*) FILTER (WHERE classificacao_viabilidade = 'Sem Dados Salariais') AS sem_dados
            FROM main.fct_viabilidade_vagas
            """
        ).fetchone()
        labels = [
            "total",
            "declarados",
            "estimados",
            "via_cargo_provincia",
            "via_empresa",
            "via_cargo_nacional",
            "via_provincia",
            "via_nacional",
            "sem_dados",
        ]
        for label, val in zip(labels, cobertura):
            print(f"  {label}: {val}")
        print()

        print("=" * 72)
        print("3) Amostra estimada (3 linhas) — fct_viabilidade_vagas")
        print("=" * 72)
        sample_rows = con.execute(
            """
            SELECT
                titulo_cargo,
                salario_declarado,
                salario_bruto_anual,
                familia_cargo,
                fonte_salario,
                confianca_salario,
                motivo_salario_estimado,
                tamanho_amostra_salario,
                aviso_salario,
                classificacao_viabilidade
            FROM main.fct_viabilidade_vagas
            WHERE salario_estimado = true
            LIMIT 3
            """
        ).fetchall()

        if not sample_rows:
            print("Nenhuma vaga com salário estimado.")
        else:
            for i, row in enumerate(sample_rows, start=1):
                (
                    titulo,
                    declarado,
                    bruto,
                    familia,
                    fonte,
                    confianca,
                    motivo,
                    amostra,
                    aviso,
                    classificacao,
                ) = row
                print(f"  [{i}] titulo_cargo: {titulo}")
                print(f"      salario_declarado: {declarado}")
                print(f"      salario_bruto_anual: {bruto}*")
                print(f"      familia_cargo: {familia}")
                print(f"      fonte_salario: {fonte}")
                print(f"      confianca_salario: {confianca}")
                print(f"      motivo_salario_estimado: {motivo}")
                print(f"      tamanho_amostra_salario: {amostra}")
                print(f"      aviso_salario: {aviso}")
                print(f"      classificacao_viabilidade: {classificacao}")
                print()

    finally:
        con.close()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
