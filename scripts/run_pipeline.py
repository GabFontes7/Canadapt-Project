"""
CanAdapt — orquestrador local / CI do pipeline Medalhão.

Ordem:
  1) ingestão Adzuna (Bronze)
  2) ingestão custo de vida (Bronze)
  3) ingestão de salários oficiais (Bronze)
  4) processamento Silver (vagas, custos e salários)
  5) enriquecimento NOC/senioridade das vagas
  6) pesquisa salarial com fonte verificável
  7) dbt run (Gold)
  8) publicação do Gold canônico em Parquet (local + S3)
  9) validação rápida do Gold
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[1]
RUN_EVENTS: list[dict] = []


def _run(label: str, command: list[str], *, skip: bool = False) -> None:
    started = time.monotonic()
    print("=" * 72)
    print(f"STEP | {label}")
    print(f"CMD  | {' '.join(command)}")
    print("=" * 72)
    if skip:
        print(f"SKIP | {label}")
        RUN_EVENTS.append({"step": label, "status": "skipped", "duration_seconds": 0})
        return
    try:
        subprocess.run(command, cwd=PROJECT_ROOT, check=True)
        RUN_EVENTS.append(
            {
                "step": label,
                "status": "success",
                "duration_seconds": round(time.monotonic() - started, 3),
            }
        )
    except subprocess.CalledProcessError:
        RUN_EVENTS.append(
            {
                "step": label,
                "status": "failed",
                "duration_seconds": round(time.monotonic() - started, 3),
            }
        )
        raise


def _collect_quality_metrics() -> tuple[dict, list[str]]:
    db_path = PROJECT_ROOT / "data" / "gold" / "canadapt_analytics.duckdb"
    metrics: dict = {}
    alerts: list[str] = []
    if not db_path.exists():
        return metrics, ["Gold DuckDB not found for quality metrics"]

    try:
        with duckdb.connect(str(db_path), read_only=True) as con:
            row = con.execute(
                """
                select
                    count(*) as vagas_atuais,
                    count(*) filter (where ranking_confiavel) as ranking_confiavel,
                    count(*) filter (where codigo_profissao is null) as sem_profissao,
                    count(*) filter (where salario_referencia_governo_atipico) as salarios_atipicos,
                    count(*) filter (
                        where qualidade_custo_vida = 'auditavel'
                    ) as custo_auditavel,
                    count(*) filter (
                        where metodo_classificacao_profissao = 'gemini_context_fingerprint'
                    ) as profissao_contextual,
                    count(*) filter (
                        where metodo_localizacao in ('country_generic', 'remote')
                    ) as geo_generica,
                    count(*) filter (where confianca_calculo = 'baixa') as calculo_baixa
                from main.fct_viabilidade_vagas
                """
            ).fetchone()
            keys = (
                "vagas_atuais",
                "ranking_confiavel",
                "sem_profissao",
                "salarios_atipicos",
                "custo_auditavel",
                "profissao_contextual",
                "geo_generica",
                "calculo_baixa",
            )
            metrics.update(dict(zip(keys, row)))
            metrics["vagas_historicas"] = con.execute(
                "select count(*) from main.fct_vagas_snapshot"
            ).fetchone()[0]
            metrics["vagas_fechadas"] = con.execute(
                "select count(*) from main.dim_vaga where not is_current"
            ).fetchone()[0]
    except duckdb.Error as exc:
        return metrics, [f"Could not collect Gold metrics: {exc}"]

    total = metrics.get("vagas_atuais", 0)
    eligible = metrics.get("ranking_confiavel", 0)
    metrics["pct_ranking_confiavel"] = round(100 * eligible / total, 2) if total else 0
    if metrics.get("sem_profissao", 0):
        alerts.append(f"{metrics['sem_profissao']} current jobs without profession code")
    if metrics.get("salarios_atipicos", 0):
        alerts.append(f"{metrics['salarios_atipicos']} government salary outliers")
    expected_auditable = total - metrics.get("geo_generica", 0)
    if metrics.get("custo_auditavel", 0) < expected_auditable:
        alerts.append("Some localized jobs lack auditable CMHC/StatCan costs")
    if total and metrics.get("profissao_contextual", 0) / total < 0.8:
        alerts.append("Less than 80% of current jobs use contextual profession v2")
    if total and metrics["pct_ranking_confiavel"] < 5:
        alerts.append("Less than 5% of current jobs are ranking-eligible")
    return metrics, alerts


def _write_run_manifest(status: str) -> Path:
    run_id = os.environ.get("CANADAPT_RUN_ID", "unknown")
    out_dir = PROJECT_ROOT / "data" / "metadata" / "runs"
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / f"{run_id}.json"
    metrics, alerts = _collect_quality_metrics()
    path.write_text(
        json.dumps(
            {
                "run_id": run_id,
                "status": status,
                "finished_at_utc": datetime.now(timezone.utc).isoformat(),
                "steps": RUN_EVENTS,
                "quality_metrics": metrics,
                "alerts": alerts,
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def main() -> int:
    parser = argparse.ArgumentParser(description="CanAdapt weekly/local pipeline runner")
    parser.add_argument("--skip-adzuna", action="store_true", help="Pula ingestão Adzuna")
    parser.add_argument("--skip-col", action="store_true", help="Pula ingestão de custo de vida")
    parser.add_argument("--skip-wages", action="store_true", help="Pula salários oficiais")
    parser.add_argument("--skip-silver", action="store_true", help="Pula processamento Silver")
    parser.add_argument("--skip-enrich", action="store_true", help="Pula enriquecimento NOC")
    parser.add_argument("--skip-dbt", action="store_true", help="Pula dbt run")
    parser.add_argument("--skip-publish", action="store_true", help="Pula publicação Gold")
    parser.add_argument("--skip-validate", action="store_true", help="Pula verificar_gold.py")
    parser.add_argument(
        "--silver-partition",
        default="",
        help='Override dbt, ex: year=2026/month=07/day=15',
    )
    args = parser.parse_args()

    run_id = os.getenv("CANADAPT_RUN_ID") or str(uuid.uuid4())
    os.environ["CANADAPT_RUN_ID"] = run_id
    print(f"CANADAPT_RUN_ID={run_id}")

    python = sys.executable
    dbt_executable = shutil.which("dbt")
    if not dbt_executable:
        executable_name = "dbt.exe" if os.name == "nt" else "dbt"
        dbt_executable = str(Path(sys.executable).with_name(executable_name))
    (PROJECT_ROOT / "data" / "gold").mkdir(parents=True, exist_ok=True)

    _run(
        "Ingestao Adzuna -> Bronze",
        [python, "src/ingestion/ingest_adzuna.py"],
        skip=args.skip_adzuna,
    )
    _run(
        "Ingestao custo de vida -> Bronze",
        [python, "src/ingestion/ingest_cost_of_living.py"],
        skip=args.skip_col,
    )
    _run(
        "Ingestao salarios oficiais -> Bronze",
        [python, "src/ingestion/ingest_wages.py"],
        skip=args.skip_wages,
    )
    _run(
        "Contratos Bronze",
        [python, "src/quality/validate_bronze.py"],
        skip=args.skip_adzuna and args.skip_col and args.skip_wages,
    )
    _run(
        "Processamento Silver",
        [python, "src/processing/process_silver.py"],
        skip=args.skip_silver,
    )
    _run(
        "Processamento salarios oficiais -> Silver",
        [python, "src/processing/process_wages.py"],
        skip=args.skip_wages,
    )
    _run(
        "Enriquecimento NOC e senioridade",
        [python, "src/processing/enrich_jobs.py"],
        skip=args.skip_enrich,
    )
    _run(
        "Pesquisa salarial com fonte verificavel",
        [python, "src/processing/enrich_salary_research.py"],
        skip=args.skip_enrich,
    )

    _run(
        "dbt seed -> referencias oficiais",
        [dbt_executable, "seed", "--profiles-dir", "."],
        skip=args.skip_dbt,
    )
    dbt_cmd = [dbt_executable, "run", "--profiles-dir", "."]
    if args.silver_partition:
        dbt_cmd.extend(["--vars", f'{{"silver_partition": "{args.silver_partition}"}}'])
    _run("dbt run -> Gold", dbt_cmd, skip=args.skip_dbt)
    _run(
        "dbt test -> qualidade Gold",
        [dbt_executable, "test", "--profiles-dir", "."],
        skip=args.skip_dbt,
    )

    _run(
        "Publicacao Gold Parquet -> local + S3",
        [python, "src/publishing/publish_gold.py"],
        skip=args.skip_publish,
    )

    _run(
        "Validacao Gold",
        [python, "verificar_gold.py"],
        skip=args.skip_validate,
    )

    manifest = _write_run_manifest("success")
    print(f"\nPipeline CanAdapt finalizado com sucesso. Manifest: {manifest}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as exc:
        manifest = _write_run_manifest("failed")
        print(f"\nERRO: etapa falhou com exit code {exc.returncode}", file=sys.stderr)
        print(f"Manifest: {manifest}", file=sys.stderr)
        raise SystemExit(exc.returncode)
