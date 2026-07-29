"""Validate lightweight contracts for the latest Bronze source files."""

from __future__ import annotations

import csv
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
REPORT_PATH = PROJECT_ROOT / "data" / "metadata" / "quality" / "bronze_contract.json"


def _latest(pattern: str) -> Path:
    files = list(BRONZE_ROOT.rglob(pattern))
    if not files:
        raise FileNotFoundError(f"No Bronze file found: {pattern}")
    return max(files, key=lambda path: path.stat().st_mtime)


def _validate_adzuna(path: Path) -> dict[str, Any]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    errors: list[str] = []
    for field in ("source", "country", "extracted_at_utc", "payload"):
        if field not in doc:
            errors.append(f"missing envelope field: {field}")

    results = (doc.get("payload") or {}).get("results")
    if not isinstance(results, list) or not results:
        errors.append("payload.results must be a non-empty list")
        results = []

    required = ("id", "title", "redirect_url")
    invalid = [
        index
        for index, job in enumerate(results)
        if any(not job.get(field) for field in required)
    ]
    ids = [str(job.get("id")) for job in results if job.get("id")]
    duplicate_ids = len(ids) - len(set(ids))
    if invalid:
        errors.append(f"{len(invalid)} jobs missing id/title/redirect_url")
    if duplicate_ids:
        errors.append(f"{duplicate_ids} duplicate job IDs")

    return {
        "file": str(path),
        "rows": len(results),
        "invalid_rows": len(invalid),
        "duplicate_ids": duplicate_ids,
        "errors": errors,
    }


def _validate_cost_of_living(path: Path) -> dict[str, Any]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    provinces = doc.get("provincias") or []
    cities = [city for province in provinces for city in province.get("cidades", [])]
    errors: list[str] = []
    methodology = doc.get("metodologia_versao")
    if methodology != "official_sources_v2":
        errors.append(
            f"expected metodologia_versao=official_sources_v2, got {methodology!r}"
        )
    if len(provinces) != 10:
        errors.append(f"expected 10 provinces, got {len(provinces)}")
    if len(cities) < 17:
        errors.append(f"expected at least 17 cities, got {len(cities)}")
    invalid = [
        city
        for city in cities
        if not city.get("nome_cidade")
        or not isinstance(city.get("aluguel_medio_1bdr"), (int, float))
        or not isinstance(city.get("custo_vida_sem_aluguel"), (int, float))
    ]
    if invalid:
        errors.append(f"{len(invalid)} cities with invalid costs")
    invalid_sources = [
        city
        for city in cities
        if "cmhc-schl.gc.ca" not in str(city.get("fonte_moradia_url") or "")
        or "statcan.gc.ca" not in str(city.get("fonte_custo_vida_url") or "")
        or city.get("metodo_custo_vida") != "statcan_shs_cpi_provincial"
        or city.get("custo_vida_estimado") is not True
        or not isinstance(city.get("custo_base_provincial_2023"), (int, float))
        or not isinstance(city.get("fator_domicilio_unipessoal"), (int, float))
        or not isinstance(city.get("fator_cpi_2026"), (int, float))
        or not city.get("cpi_mes_referencia")
    ]
    if invalid_sources:
        errors.append(f"{len(invalid_sources)} cities without auditable official sources")
    province_codes = [province.get("sigla_provincia") for province in provinces]
    expected_codes = {"ON", "BC", "AB", "QC", "NS", "MB", "SK", "NB", "PE", "NL"}
    if set(province_codes) != expected_codes:
        errors.append("province identity set does not match the 10-province contract")
    if len(province_codes) != len(set(province_codes)):
        errors.append("duplicate province codes")
    return {
        "file": str(path),
        "provinces": len(provinces),
        "cities": len(cities),
        "invalid_rows": len(invalid),
        "invalid_sources": len(invalid_sources),
        "methodology": methodology,
        "errors": errors,
    }


def _validate_wages(path: Path) -> dict[str, Any]:
    required = {
        "NOC_CNP",
        "NOC_Title_eng",
        "prov",
        "Median_Wage_Salaire_Median",
        "Annual_Wage_Flag_Salaire_annuel",
    }
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        missing = sorted(required - set(reader.fieldnames or []))
        rows = sum(1 for _ in reader)
    errors = [f"missing CSV columns: {missing}"] if missing else []
    if rows == 0:
        errors.append("wage CSV has no rows")
    return {"file": str(path), "rows": rows, "errors": errors}


def main() -> int:
    report: dict[str, Any] = {
        "validated_at_utc": datetime.now(timezone.utc).isoformat(),
        "run_id": __import__("os").getenv("CANADAPT_RUN_ID", "local-manual"),
        "sources": {},
    }
    try:
        report["sources"]["adzuna"] = _validate_adzuna(_latest("adzuna_raw_*.json"))
        report["sources"]["cost_of_living"] = _validate_cost_of_living(
            _latest("cost_of_living.json")
        )
        report["sources"]["wages"] = _validate_wages(_latest("wages_official.csv"))
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        report["fatal_error"] = str(exc)

    errors = [
        error
        for source in report["sources"].values()
        for error in source.get("errors", [])
    ]
    if report.get("fatal_error"):
        errors.append(report["fatal_error"])
    report["status"] = "pass" if not errors else "fail"
    report["errors"] = errors

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        json.dumps(report, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0 if report["status"] == "pass" else 1


if __name__ == "__main__":
    sys.exit(main())
