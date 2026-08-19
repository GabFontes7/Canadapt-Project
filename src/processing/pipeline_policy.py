"""Shared data-engineering policy: contracts, Gemini degradation, run evidence."""

from __future__ import annotations

import json
import os
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

PROJECT_ROOT = Path(__file__).resolve().parents[2]
STEP_METRICS_DIR = PROJECT_ROOT / "data" / "metadata" / "steps"

# Bump when Silver/Gold meaning changes (new required columns, prompt semantics).
DATA_CONTRACT_VERSION = "1.1.0"
GEO_PROMPT_VERSION = "geo_official_cities_v1"
NOC_PROMPT_VERSION = "noc_context_v2"
SALARY_RESEARCH_PROMPT_VERSION = "salary_research_v2"
SILVER_JOBS_SCHEMA_VERSION = "jobs_clean_v1"

# How long CI restores hive partitions and how long S3 may keep raw Bronze/Silver jobs.
DEFAULT_LAKE_RETENTION_DAYS = 90

UNMAPPED_GEO = {
    "cidade": "Remote",
    "provincia": "Remote",
    "cma": "Remote",
    "metodo": "gemini_unavailable",
    "confianca": 0.0,
}

HIVE_DATE_RE = re.compile(
    r"year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/"
)


def hive_date_from_key(key: str) -> date | None:
    match = HIVE_DATE_RE.search(key)
    if not match:
        return None
    try:
        return date(
            int(match.group("year")),
            int(match.group("month")),
            int(match.group("day")),
        )
    except ValueError:
        return None


def lake_retention_days() -> int:
    raw = os.getenv("CANADAPT_LAKE_RETENTION_DAYS", str(DEFAULT_LAKE_RETENTION_DAYS))
    try:
        days = int(raw)
    except ValueError:
        days = DEFAULT_LAKE_RETENTION_DAYS
    return max(7, days)


def allow_gemini_degraded() -> bool:
    raw = os.getenv("CANADAPT_ALLOW_GEMINI_DEGRADED", "1").strip().lower()
    return raw not in {"0", "false", "no"}


def gemini_exception_types() -> tuple[type[BaseException], ...]:
    types: list[type[BaseException]] = [RuntimeError]
    try:
        from google.genai import errors as genai_errors
    except ImportError:
        return tuple(types)
    for name in ("APIError", "ClientError", "ServerError"):
        cls = getattr(genai_errors, name, None)
        if isinstance(cls, type) and issubclass(cls, BaseException):
            types.append(cls)
    return tuple(types)


def is_gemini_unavailable(exc: BaseException) -> bool:
    text = str(exc).upper()
    markers = (
        "503",
        "UNAVAILABLE",
        "HIGH DEMAND",
        "RESOURCE_EXHAUSTED",
        "429",
        "UNAVAILABLE.",
    )
    return any(marker in text for marker in markers)


def contract_stamp() -> dict[str, str]:
    return {
        "data_contract_version": DATA_CONTRACT_VERSION,
        "silver_jobs_schema_version": SILVER_JOBS_SCHEMA_VERSION,
        "geo_prompt_version": GEO_PROMPT_VERSION,
    }


def hive_prefixes_for_day(day: date) -> list[str]:
    year = f"year={day.year}"
    month = f"month={day.month:02d}"
    day_p = f"day={day.day:02d}"
    hive = f"{year}/{month}/{day_p}/"
    return [
        f"bronze/{hive}",
        f"bronze/jooble/{hive}",
        f"bronze/cost_of_living/{hive}",
        f"silver/jobs/{hive}",
        f"silver/cost_of_living/{hive}",
    ]


def retained_hive_prefixes(as_of: date | None = None, days: int | None = None) -> list[str]:
    end = as_of or datetime.now(timezone.utc).date()
    window = days if days is not None else lake_retention_days()
    prefixes: list[str] = []
    for offset in range(window):
        prefixes.extend(hive_prefixes_for_day(end - timedelta(days=offset)))
    return prefixes


def durable_prefixes() -> list[str]:
    """Caches, wages and Gold — not expired with weekly job snapshots."""
    return [
        "bronze/wages/",
        "silver/wages/",
        "silver/metadata/",
    ]


def write_step_metrics(step: str, payload: dict[str, Any]) -> Path:
    run_id = os.environ.get("CANADAPT_RUN_ID", "unknown")
    STEP_METRICS_DIR.mkdir(parents=True, exist_ok=True)
    safe_step = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in step)
    path = STEP_METRICS_DIR / f"{run_id}-{safe_step}.json"
    body = {
        "run_id": run_id,
        "step": step,
        "data_contract_version": DATA_CONTRACT_VERSION,
        "finished_at_utc": datetime.now(timezone.utc).isoformat(),
        **payload,
    }
    path.write_text(json.dumps(body, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def load_step_metrics(run_id: str) -> list[dict[str, Any]]:
    if not STEP_METRICS_DIR.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(STEP_METRICS_DIR.glob(f"{run_id}-*.json")):
        try:
            rows.append(json.loads(path.read_text(encoding="utf-8")))
        except json.JSONDecodeError:
            continue
    return rows
