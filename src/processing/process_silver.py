"""
CanAdapt — Silver processing (geo standardization + Dual-Write Parquet).

Reads Bronze Adzuna + Jooble jobs and cost-of-living, standardizes locations
via Gemini with Cache-Aside + model-cost fallback, deduplicates sources, and
writes clean Parquet tables to local Silver and AWS S3.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, TypeVar

import boto3
import duckdb
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv
from google import genai
from google.genai import types
from google.genai.errors import ClientError as GeminiClientError
from pydantic import BaseModel, Field, ValidationError

from pipeline_policy import (
    DATA_CONTRACT_VERSION,
    GEO_PROMPT_VERSION,
    UNMAPPED_GEO,
    allow_gemini_degraded,
    contract_stamp,
    write_step_metrics,
)

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
GEO_CACHE_PATH = SILVER_ROOT / "metadata" / "geo_cache.json"

# Cheapest → most capable (string without "models/" prefix for the SDK).
GEMINI_MODEL_FALLBACK_QUEUE = (
    "gemini-3.1-flash-lite",
    "gemini-3.5-flash-lite",
    "gemini-3.5-flash",
    "gemini-3.6-flash",
)

GEO_BATCH_SIZE = 40

OFFICIAL_CITY_TO_PROVINCE: dict[str, str] = {
    "Toronto": "ON",
    "Ottawa": "ON",
    "Waterloo": "ON",
    "Vancouver": "BC",
    "Victoria": "BC",
    "Calgary": "AB",
    "Edmonton": "AB",
    "Montréal": "QC",
    "Québec City": "QC",
    "Halifax": "NS",
    "Winnipeg": "MB",
    "Saskatoon": "SK",
    "Regina": "SK",
    "Moncton": "NB",
    "Fredericton": "NB",
    "Charlottetown": "PE",
    "St. John's": "NL",
    "Remote": "Remote",
}

GEO_SYSTEM_INSTRUCTION = (
    "Você é um agente de padronização geográfica canadense. Seu trabalho é "
    "associar localizações brutas de vagas às nossas 17 cidades oficiais: "
    "Toronto, Ottawa, Waterloo, Vancouver, Victoria, Calgary, Edmonton, "
    "Montréal, Québec City, Halifax, Winnipeg, Saskatoon, Regina, Moncton, "
    "Fredericton, Charlottetown, St. John's (e suas respectivas siglas de "
    "províncias: ON, BC, AB, QC, NS, MB, SK, NB, PE, NL). "
    "Se o termo original for remoto ou genérico do país, use 'Remote' para "
    "ambos os campos. Se for uma cidade satélite, mapeie para a cidade de "
    "referência/CMA mais próxima de nossa lista. Preencha cma_padronizada com "
    "essa cidade de referência, metodo_mapeamento com exact, satellite, remote "
    "ou country_generic e uma confiança entre 0 e 1."
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.processing.silver")

TSchema = TypeVar("TSchema", bound=BaseModel)


# -----------------------------------------------------------------------------
# Pydantic contracts
# -----------------------------------------------------------------------------


class GeoMapping(BaseModel):
    termo_original: str
    cidade_padronizada: str = Field(
        description="Uma das 17 cidades oficiais ou Remote"
    )
    provincia_padronizada: str = Field(
        description="Uma das 10 siglas de província ou Remote"
    )
    cma_padronizada: str = Field(
        description="CMA/cidade de referência da lista oficial ou Remote"
    )
    metodo_mapeamento: str = Field(
        description="exact, satellite, remote ou country_generic"
    )
    confianca: float = Field(ge=0, le=1)


class GeoMappingPayload(BaseModel):
    mapeamentos: list[GeoMapping]


# -----------------------------------------------------------------------------
# Env / S3 helpers
# -----------------------------------------------------------------------------


def load_env() -> None:
    load_dotenv(PROJECT_ROOT / ".env")


def require_gemini_api_key() -> str:
    api_key = os.getenv("GEMINI_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError("Missing GEMINI_API_KEY in the project .env file.")
    return api_key


def resolve_s3_bucket_name() -> str:
    bucket = (
        os.getenv("AWS_BUCKET_NAME", "").strip()
        or os.getenv("AWS_S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        raise EnvironmentError(
            "Missing AWS_BUCKET_NAME (or fallback AWS_S3_BUCKET_NAME) in .env."
        )
    return bucket


def build_s3_client():
    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "").strip() or "us-east-1"
    if not access_key or not secret_key:
        raise EnvironmentError(
            "Missing AWS_ACCESS_KEY_ID or AWS_SECRET_ACCESS_KEY in .env."
        )
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )


def upload_file_to_s3(local_path: Path, s3_key: str) -> str:
    bucket = resolve_s3_bucket_name()
    try:
        s3 = build_s3_client()
        if local_path.suffix.lower() == ".parquet":
            s3.upload_file(
                str(local_path),
                bucket,
                s3_key,
                ExtraArgs={"ContentType": "application/octet-stream"},
            )
        else:
            s3.upload_file(str(local_path), bucket, s3_key)
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"S3 upload failed ({code}) for s3://{bucket}/{s3_key}: {exc}"
        ) from exc
    except BotoCoreError as exc:
        raise RuntimeError(f"AWS/boto3 error uploading {local_path}: {exc}") from exc

    uri = f"s3://{bucket}/{s3_key}"
    logger.info("S3 uploaded: %s", uri)
    return uri


# -----------------------------------------------------------------------------
# Path resolution (latest Bronze day + matching Silver partitions)
# -----------------------------------------------------------------------------


def resolve_latest_adzuna_file() -> Path:
    files = sorted(
        BRONZE_ROOT.rglob("adzuna_raw_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            f"No Adzuna bronze files found under {BRONZE_ROOT}"
        )
    return files[0]


def resolve_jooble_file_for_partition(
    year_p: str,
    month_p: str,
    day_p: str,
) -> Path | None:
    """Return the latest Jooble Bronze file for the Adzuna snapshot day."""
    directory = BRONZE_ROOT / "jooble" / year_p / month_p / day_p
    files = sorted(
        directory.glob("jooble_raw_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def resolve_jobbank_file_for_partition(
    year_p: str,
    month_p: str,
    day_p: str,
) -> Path | None:
    """Return the latest Job Bank Bronze file for the Adzuna snapshot day."""
    directory = BRONZE_ROOT / "jobbank" / year_p / month_p / day_p
    files = sorted(
        directory.glob("jobbank_raw_*.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return files[0] if files else None


def resolve_cost_of_living_file(preferred_day_dir: Path) -> Path:
    candidate = preferred_day_dir / "cost_of_living.json"
    if candidate.exists():
        return candidate
    files = sorted(
        BRONZE_ROOT.rglob("cost_of_living.json"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    if not files:
        raise FileNotFoundError(
            f"No cost_of_living.json found under {BRONZE_ROOT}"
        )
    return files[0]


def partition_segments_from_bronze_path(adzuna_path: Path) -> tuple[str, str, str]:
    """Infer year=/month=/day= from .../year=YYYY/month=MM/day=DD/file.json."""
    day_dir = adzuna_path.parent
    parts = {p.split("=", 1)[0]: p for p in day_dir.parts if "=" in p}
    for key in ("year", "month", "day"):
        if key not in parts:
            now = datetime.now().astimezone()
            return (
                f"year={now.year}",
                f"month={now.month:02d}",
                f"day={now.day:02d}",
            )
    return parts["year"], parts["month"], parts["day"]


# -----------------------------------------------------------------------------
# Geo cache (Cache-Aside)
# -----------------------------------------------------------------------------


def load_geo_cache(path: Path = GEO_CACHE_PATH) -> dict[str, dict[str, Any]]:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text("{}", encoding="utf-8")
        logger.info("Created empty geo cache: %s", path)
        return {}
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid geo cache JSON at {path}: {exc}") from exc
    if not isinstance(raw, dict):
        raise RuntimeError(f"Geo cache must be a JSON object at {path}")
    return raw


def save_geo_cache(
    cache: dict[str, dict[str, Any]],
    path: Path = GEO_CACHE_PATH,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    logger.info("Geo cache saved | entries=%d | path=%s", len(cache), path)


# -----------------------------------------------------------------------------
# Gemini fallback caller
# -----------------------------------------------------------------------------


def _normalize_model_name(model: str) -> str:
    name = model.strip()
    if name.startswith("models/"):
        name = name[len("models/") :]
    return name


def call_gemini_with_fallback(
    prompt: str,
    response_schema: type[TSchema],
    *,
    system_instruction: str | None = None,
) -> tuple[TSchema, str]:
    """
    Try Gemini models from cheapest to richest.
    Returns (validated_payload, model_name_used).
    """
    api_key = require_gemini_api_key()
    client = genai.Client(api_key=api_key)
    errors: list[str] = []

    config_kwargs: dict[str, Any] = {
        "response_mime_type": "application/json",
        "response_schema": response_schema,
        "temperature": 0.1,
    }
    if system_instruction:
        config_kwargs["system_instruction"] = system_instruction

    for model in GEMINI_MODEL_FALLBACK_QUEUE:
        model_id = _normalize_model_name(model)
        try:
            logger.info("Gemini attempt | model=%s | schema=%s", model_id, response_schema.__name__)
            response = client.models.generate_content(
                model=model_id,
                contents=prompt,
                config=types.GenerateContentConfig(**config_kwargs),
            )
            raw = (response.text or "").strip()
            if not raw:
                raise RuntimeError("empty response.text")
            payload = response_schema.model_validate_json(raw)
            logger.info("Gemini success | model=%s", model_id)
            return payload, model_id
        except (GeminiClientError, ValidationError, RuntimeError, ValueError) as exc:
            msg = f"{model_id}: {exc}"
            errors.append(msg)
            logger.warning(
                "Gemini fallback advancing | failed_model=%s | error=%s",
                model_id,
                exc,
            )
            continue
        except Exception as exc:  # noqa: BLE001
            msg = f"{model_id}: {exc}"
            errors.append(msg)
            logger.warning(
                "Gemini fallback advancing | failed_model=%s | error=%s",
                model_id,
                exc,
            )
            continue

    raise RuntimeError(
        "All Gemini models failed in fallback queue. Errors: " + " | ".join(errors)
    )


def normalize_geo_mapping(item: GeoMapping) -> dict[str, Any]:
    """Light post-validation against the closed city list."""
    city = item.cidade_padronizada.strip()
    province = item.provincia_padronizada.strip()

    # Accent / alias soft-fixes
    aliases = {
        "Montreal": "Montréal",
        "Quebec City": "Québec City",
        "Quebec": "Québec City",
        "St Johns": "St. John's",
        "St. Johns": "St. John's",
    }
    city = aliases.get(city, city)

    if city not in OFFICIAL_CITY_TO_PROVINCE:
        # Satellite / unknown → Remote rather than inventing cities
        logger.warning(
            "Unknown standardized city %r for term %r → Remote",
            city,
            item.termo_original,
        )
        return {
            "cidade": "Remote",
            "provincia": "Remote",
            "cma": "Remote",
            "metodo": "fallback_unknown",
            "confianca": 0.0,
        }

    expected_prov = OFFICIAL_CITY_TO_PROVINCE[city]
    if province != expected_prov and city != "Remote":
        province = expected_prov

    method = item.metodo_mapeamento.strip().lower()
    if method not in {"exact", "satellite", "remote", "country_generic"}:
        method = "model_other"
    return {
        "cidade": city,
        "provincia": province,
        "cma": city if city != "Remote" else "Remote",
        "metodo": method,
        "confianca": round(float(item.confianca), 4),
    }


def gemini_map_locations(terms: list[str]) -> tuple[dict[str, dict[str, Any]], str]:
    """Batch-map raw location strings via Gemini fallback. Returns (partial_cache, model)."""
    if not terms:
        return {}, ""

    mappings: dict[str, dict[str, Any]] = {}
    model_used = ""
    degraded = False

    for i in range(0, len(terms), GEO_BATCH_SIZE):
        batch = terms[i : i + GEO_BATCH_SIZE]
        prompt = (
            "Padronize as localizações brutas abaixo para o contrato GeoMappingPayload.\n"
            "Retorne exatamente um mapeamento por termo_original (mesma string).\n\n"
            "TERMOS:\n"
            + "\n".join(f"- {t}" for t in batch)
        )
        try:
            payload, model_used = call_gemini_with_fallback(
                prompt,
                GeoMappingPayload,
                system_instruction=GEO_SYSTEM_INSTRUCTION,
            )
        except RuntimeError as exc:
            if not allow_gemini_degraded():
                raise
            degraded = True
            logger.warning(
                "Geo Gemini unavailable; mapping batch to Remote | start=%d | %s",
                i,
                exc,
            )
            for term in batch:
                mappings[term] = dict(UNMAPPED_GEO)
            continue
        for item in payload.mapeamentos:
            mappings[item.termo_original] = normalize_geo_mapping(item)

        # Ensure every requested term exists even if model skipped some
        for term in batch:
            if term not in mappings:
                logger.warning("Gemini omitted term %r → Remote", term)
                mappings[term] = {
                    "cidade": "Remote",
                    "provincia": "Remote",
                    "cma": "Remote",
                    "metodo": "model_omitted",
                    "confianca": 0.0,
                }

    if degraded:
        model_used = model_used or "degraded-cache-only"
    return mappings, model_used


# -----------------------------------------------------------------------------
# DuckDB extraction / transforms
# -----------------------------------------------------------------------------


def extract_distinct_locations_duckdb(con: duckdb.DuckDBPyConnection, adzuna_path: Path) -> list[str]:
    """Distinct raw Adzuna location.display_name values from Bronze JSON."""
    path = adzuna_path.as_posix()
    try:
        rows = con.execute(
            """
            WITH jobs AS (
              SELECT unnest(payload.results) AS j
              FROM read_json_auto(?)
            )
            SELECT DISTINCT CAST(j.location.display_name AS VARCHAR) AS termo_bruto
            FROM jobs
            WHERE j.location.display_name IS NOT NULL
              AND trim(CAST(j.location.display_name AS VARCHAR)) <> ''
            ORDER BY 1
            """,
            [path],
        ).fetchall()
        return [r[0] for r in rows]
    except Exception as duck_exc:
        logger.warning(
            "DuckDB nested extract failed (%s). Falling back to Python JSON parse.",
            duck_exc,
        )
        try:
            doc = json.loads(adzuna_path.read_text(encoding="utf-8"))
            results = (doc.get("payload") or {}).get("results") or []
            terms = sorted(
                {
                    (j.get("location") or {}).get("display_name")
                    for j in results
                    if (j.get("location") or {}).get("display_name")
                }
            )
            return [t for t in terms if str(t).strip()]
        except Exception as py_exc:
            raise RuntimeError(
                f"Failed to extract locations from {adzuna_path}: {py_exc}"
            ) from py_exc


def _clean_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "")).strip()


def _as_bool(value: Any) -> bool:
    return str(value or "").strip().casefold() in {"1", "true", "yes"}


def _dedupe_fingerprint(row: dict[str, Any]) -> str:
    """Cross-source fingerprint for the same employer/title/location."""
    canonical = "|".join(
        _clean_text(row.get(field)).casefold()
        for field in ("title", "company", "location_raw")
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def normalize_adzuna_rows(path: Path, run_id: str) -> list[dict[str, Any]]:
    doc = json.loads(path.read_text(encoding="utf-8"))
    extracted_at = doc.get("extracted_at_utc")
    rows: list[dict[str, Any]] = []
    for job in (doc.get("payload") or {}).get("results") or []:
        source_id = _clean_text(job.get("id"))
        if not source_id:
            continue
        rows.append(
            {
                "job_id": source_id,
                "source": "adzuna",
                "source_job_id": source_id,
                "source_site": "Adzuna",
                "title": job.get("title"),
                "company": (job.get("company") or {}).get("display_name"),
                "location_raw": (job.get("location") or {}).get("display_name"),
                "category": (job.get("category") or {}).get("label"),
                "focus_area": None,
                "mobility_signals": job.get("canadapt_mobility_signals") or [],
                "filter_version": job.get("canadapt_filter_version") or "adzuna_mobility_text_v2",
                "job_type": None,
                "description": job.get("description"),
                "created": job.get("created"),
                "redirect_url": job.get("redirect_url"),
                "salary_raw": None,
                "salary_min": job.get("salary_min"),
                "salary_max": job.get("salary_max"),
                "salary_is_predicted": _as_bool(job.get("salary_is_predicted")),
                "extracted_at_utc": extracted_at,
                "pipeline_run_id": run_id,
            }
        )
    return rows


def normalize_jooble_rows(path: Path | None, run_id: str) -> list[dict[str, Any]]:
    if path is None:
        return []
    doc = json.loads(path.read_text(encoding="utf-8"))
    extracted_at = doc.get("extracted_at_utc")
    rows: list[dict[str, Any]] = []
    for job in (doc.get("payload") or {}).get("results") or []:
        source_id = _clean_text(job.get("id"))
        if not source_id:
            continue
        rows.append(
            {
                "job_id": f"jooble:{source_id}",
                "source": "jooble",
                "source_job_id": source_id,
                "source_site": job.get("source") or "Jooble",
                "title": job.get("title"),
                "company": job.get("company"),
                "location_raw": job.get("location"),
                "category": job.get("canadapt_area"),
                "focus_area": job.get("canadapt_area"),
                "mobility_signals": job.get("canadapt_mobility_signals") or [],
                "filter_version": job.get("canadapt_filter_version"),
                "job_type": job.get("type"),
                "description": job.get("snippet"),
                "created": job.get("updated"),
                "redirect_url": job.get("link"),
                # Jooble exposes salary as free text without a reliable period.
                # Preserve it for audit, but do not annualize ambiguous values.
                "salary_raw": job.get("salary"),
                "salary_min": None,
                "salary_max": None,
                "salary_is_predicted": False,
                "extracted_at_utc": extracted_at,
                "pipeline_run_id": run_id,
            }
        )
    return rows


def _parse_jobbank_salary(salary_text: object) -> tuple[float | None, float | None]:
    """Best-effort annualization of Job Bank salary strings."""
    text = _clean_text(salary_text).lower().replace(",", "")
    if not text:
        return None, None
    amounts = [float(match) for match in re.findall(r"\$?\s*(\d+(?:\.\d+)?)", text)]
    if not amounts:
        return None, None
    low = amounts[0]
    high = amounts[1] if len(amounts) > 1 else amounts[0]
    if "hour" in text:
        return low * 2080.0, high * 2080.0
    if "week" in text:
        return low * 52.0, high * 52.0
    if "month" in text:
        return low * 12.0, high * 12.0
    if "year" in text or "annual" in text:
        return low, high
    # Ambiguous period — keep raw text only.
    return None, None


def _parse_jobbank_posted_date(value: object) -> str | None:
    """Convert Job Bank dates like 'August 19, 2026' to ISO timestamps."""
    text = _clean_text(value)
    if not text:
        return None
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%Y-%m-%d", "%Y-%m-%dT%H:%M:%S%z"):
        try:
            parsed = datetime.strptime(text, fmt)
            return parsed.strftime("%Y-%m-%dT00:00:00")
        except ValueError:
            continue
    return None


def normalize_jobbank_rows(path: Path | None, run_id: str) -> list[dict[str, Any]]:
    if path is None:
        return []
    doc = json.loads(path.read_text(encoding="utf-8"))
    extracted_at = doc.get("extracted_at_utc")
    rows: list[dict[str, Any]] = []
    for job in (doc.get("payload") or {}).get("results") or []:
        source_id = _clean_text(job.get("id"))
        if not source_id:
            continue
        salary_min, salary_max = _parse_jobbank_salary(job.get("salary_text"))
        rows.append(
            {
                "job_id": f"jobbank:{source_id}",
                "source": "jobbank",
                "source_job_id": source_id,
                "source_site": "Job Bank",
                "title": job.get("title"),
                "company": job.get("company"),
                "location_raw": job.get("location"),
                "category": None,
                "focus_area": None,
                "mobility_signals": job.get("canadapt_mobility_signals") or [],
                "filter_version": job.get("canadapt_filter_version")
                or "jobbank_lmia_intl_v1",
                "job_type": None,
                "description": job.get("description"),
                "created": _parse_jobbank_posted_date(job.get("posted_date")),
                "redirect_url": job.get("url"),
                "salary_raw": job.get("salary_text"),
                "salary_min": salary_min,
                "salary_max": salary_max,
                "salary_is_predicted": False,
                "noc_code": job.get("noc_code"),
                "extracted_at_utc": extracted_at,
                "pipeline_run_id": run_id,
            }
        )
    return rows


def merge_and_dedupe_job_rows(
    *row_groups: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Prefer the richer row when two sources expose the same listing."""
    selected: dict[str, dict[str, Any]] = {}
    source_priority = {"jobbank": 3, "adzuna": 2, "jooble": 1}
    for row in [item for group in row_groups for item in group]:
        fingerprint = _dedupe_fingerprint(row)
        row["dedupe_fingerprint"] = fingerprint
        current = selected.get(fingerprint)
        if current is None:
            selected[fingerprint] = row
            continue
        candidate_score = (
            source_priority.get(str(row.get("source")), 0),
            len(_clean_text(row.get("description"))),
            bool(row.get("salary_min")),
        )
        current_score = (
            source_priority.get(str(current.get("source")), 0),
            len(_clean_text(current.get("description"))),
            bool(current.get("salary_min")),
        )
        if candidate_score > current_score:
            selected[fingerprint] = row
    return list(selected.values())


def build_multisource_jobs_clean_parquet(
    con: duckdb.DuckDBPyConnection,
    rows: list[dict[str, Any]],
    geo_cache: dict[str, dict[str, Any]],
    out_path: Path,
) -> int:
    """Attach standardized geography and write the canonical Silver schema."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    enriched: list[dict[str, Any]] = []
    for row in rows:
        location = _clean_text(row.get("location_raw"))
        geo = geo_cache.get(
            location,
            {
                "cidade": "Remote",
                "provincia": "Remote",
                "cma": "Remote",
                "metodo": "fallback_unmapped",
                "confianca": 0.0,
            },
        )
        enriched.append(
            {
                **row,
                "cidade_padronizada": geo.get("cidade", "Remote"),
                "provincia_padronizada": geo.get("provincia", "Remote"),
                "cma_padronizada": geo.get("cma", geo.get("cidade", "Remote")),
                "geo_mapping_method": geo.get("metodo", "fallback_unmapped"),
                "geo_confidence": geo.get("confianca", 0.0),
                **contract_stamp(),
            }
        )
    if not enriched:
        raise RuntimeError("No valid job rows available for Silver.")

    tmp = out_path.parent / "_multisource_jobs_tmp.json"
    try:
        tmp.write_text(json.dumps(enriched, ensure_ascii=False), encoding="utf-8")
        tmp_sql = tmp.as_posix().replace("'", "''")
        out_sql = out_path.as_posix().replace("'", "''")
        con.execute(
            f"COPY (SELECT * FROM read_json_auto('{tmp_sql}')) TO '{out_sql}' "
            "(FORMAT PARQUET, COMPRESSION ZSTD)"
        )
    finally:
        tmp.unlink(missing_ok=True)
    return len(enriched)


def build_jobs_clean_parquet(
    con: duckdb.DuckDBPyConnection,
    adzuna_path: Path,
    geo_cache: dict[str, dict[str, Any]],
    out_path: Path,
) -> int:
    """Project standardized geography onto Bronze jobs and write Parquet."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = os.getenv("CANADAPT_RUN_ID", "").strip() or "local-manual"
    run_id_sql = run_id.replace("'", "''")

    mapping_rows = [
        {
            "termo_bruto": term,
            "cidade_padronizada": meta.get("cidade", "Remote"),
            "provincia_padronizada": meta.get("provincia", "Remote"),
            "cma_padronizada": meta.get("cma", meta.get("cidade", "Remote")),
            "geo_mapping_method": meta.get("metodo", "legacy_cache"),
            "geo_confidence": meta.get("confianca", 0.5),
        }
        for term, meta in geo_cache.items()
    ]

    map_json = out_path.parent / "_geo_map_tmp.json"
    try:
        map_json.write_text(
            json.dumps(mapping_rows, ensure_ascii=False),
            encoding="utf-8",
        )

        path = adzuna_path.as_posix()
        map_path = map_json.as_posix()
        out = out_path.as_posix()

        con.execute(
            f"""
            COPY (
              WITH doc AS (
                SELECT *
                FROM read_json_auto('{path}')
              ),
              jobs AS (
                SELECT
                  unnest(payload.results) AS j,
                  CAST(extracted_at_utc AS VARCHAR) AS extracted_at_utc
                FROM doc
              ),
              flat AS (
                SELECT
                  CAST(j.id AS VARCHAR) AS job_id,
                  CAST(j.title AS VARCHAR) AS title,
                  CAST(j.company.display_name AS VARCHAR) AS company,
                  CAST(j.location.display_name AS VARCHAR) AS location_raw,
                  CAST(j.category.label AS VARCHAR) AS category,
                  CAST(j.description AS VARCHAR) AS description,
                  CAST(j.created AS VARCHAR) AS created,
                  CAST(j.redirect_url AS VARCHAR) AS redirect_url,
                  TRY_CAST(j.salary_min AS DOUBLE) AS salary_min,
                  TRY_CAST(j.salary_max AS DOUBLE) AS salary_max,
                  TRY_CAST(j.salary_is_predicted AS INTEGER) AS salary_is_predicted,
                  extracted_at_utc,
                  CAST('{run_id_sql}' AS VARCHAR) AS pipeline_run_id
                FROM jobs
              ),
              geo_map AS (
                SELECT * FROM read_json_auto('{map_path}')
              )
              SELECT
                f.*,
                COALESCE(g.cidade_padronizada, 'Remote') AS cidade_padronizada,
                COALESCE(g.provincia_padronizada, 'Remote') AS provincia_padronizada,
                COALESCE(g.cma_padronizada, 'Remote') AS cma_padronizada,
                COALESCE(g.geo_mapping_method, 'fallback_unmapped') AS geo_mapping_method,
                COALESCE(g.geo_confidence, 0.0) AS geo_confidence
              FROM flat f
              LEFT JOIN geo_map g
                ON f.location_raw = g.termo_bruto
            ) TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        count = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{out}')"
        ).fetchone()[0]
        return int(count)
    except Exception as exc:
        logger.warning("Primary DuckDB jobs COPY failed (%s). Using hybrid path.", exc)
        doc = json.loads(adzuna_path.read_text(encoding="utf-8"))
        results = (doc.get("payload") or {}).get("results") or []
        extracted_at = doc.get("extracted_at_utc")
        rows: list[dict[str, Any]] = []
        for j in results:
            loc = (j.get("location") or {}).get("display_name")
            geo = geo_cache.get(
                loc or "",
                {
                    "cidade": "Remote",
                    "provincia": "Remote",
                    "cma": "Remote",
                    "metodo": "fallback_unmapped",
                    "confianca": 0.0,
                },
            )
            rows.append(
                {
                    "job_id": str(j.get("id") or ""),
                    "title": j.get("title"),
                    "company": (j.get("company") or {}).get("display_name"),
                    "location_raw": loc,
                    "category": (j.get("category") or {}).get("label"),
                    "description": j.get("description"),
                    "created": j.get("created"),
                    "redirect_url": j.get("redirect_url"),
                    "salary_min": j.get("salary_min"),
                    "salary_max": j.get("salary_max"),
                    "salary_is_predicted": j.get("salary_is_predicted"),
                    "extracted_at_utc": extracted_at,
                    "pipeline_run_id": run_id,
                    "cidade_padronizada": geo.get("cidade", "Remote"),
                    "provincia_padronizada": geo.get("provincia", "Remote"),
                    "cma_padronizada": geo.get("cma", geo.get("cidade", "Remote")),
                    "geo_mapping_method": geo.get("metodo", "legacy_cache"),
                    "geo_confidence": geo.get("confianca", 0.5),
                }
            )
        rows_json = out_path.parent / "_jobs_clean_tmp.json"
        rows_json.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        out = out_path.as_posix()
        rows_path = rows_json.as_posix()
        con.execute(
            f"COPY (SELECT * FROM read_json_auto('{rows_path}')) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        try:
            rows_json.unlink(missing_ok=True)
        except OSError:
            pass
        return len(rows)
    finally:
        try:
            map_json.unlink(missing_ok=True)
        except OSError:
            pass


def build_cost_of_living_clean_parquet(
    con: duckdb.DuckDBPyConnection,
    col_path: Path,
    out_path: Path,
) -> int:
    """Flatten CostOfLivingPayload JSON into a city-grain Parquet table."""
    out_path.parent.mkdir(parents=True, exist_ok=True)
    try:
        doc = json.loads(col_path.read_text(encoding="utf-8"))
    except Exception as exc:
        raise RuntimeError(f"Failed reading cost_of_living bronze: {exc}") from exc

    rows: list[dict[str, Any]] = []
    for prov in doc.get("provincias") or []:
        for city in prov.get("cidades") or []:
            rows.append(
                {
                    "ano_referencia": doc.get("ano_referencia"),
                    "data_execucao": doc.get("data_execucao"),
                    "consultado_em_utc": doc.get("consultado_em_utc"),
                    "metodologia_versao": doc.get("metodologia_versao"),
                    "sigla_provincia": prov.get("sigla_provincia"),
                    "nome_provincia": prov.get("nome_provincia"),
                    "aliquota_gst": prov.get("aliquota_gst"),
                    "aliquota_pst": prov.get("aliquota_pst"),
                    "aliquota_hst_total": prov.get("aliquota_hst_total"),
                    "fonte_imposto_url": prov.get("fonte_imposto_url"),
                    "vigencia_imposto": prov.get("vigencia_imposto"),
                    "nome_cidade": city.get("nome_cidade"),
                    "aluguel_medio_1bdr": city.get("aluguel_medio_1bdr"),
                    "custo_vida_sem_aluguel": city.get("custo_vida_sem_aluguel"),
                    "fonte_moradia_verificada": city.get("fonte_moradia_verificada"),
                    "fonte_custo_vida_verificada": city.get("fonte_custo_vida_verificada"),
                    "fonte_moradia_url": city.get("fonte_moradia_url"),
                    "fonte_custo_vida_url": city.get("fonte_custo_vida_url"),
                    "ano_fonte_moradia": city.get("ano_fonte_moradia"),
                    "ano_fonte_custo_vida": city.get("ano_fonte_custo_vida"),
                    "qualidade_fonte_moradia": city.get("qualidade_fonte_moradia"),
                    "metodo_custo_vida": city.get("metodo_custo_vida"),
                    "custo_vida_estimado": city.get("custo_vida_estimado"),
                    "custo_base_provincial_2023": city.get("custo_base_provincial_2023"),
                    "fator_domicilio_unipessoal": city.get("fator_domicilio_unipessoal"),
                    "fator_cpi_2026": city.get("fator_cpi_2026"),
                    "cpi_mes_referencia": city.get("cpi_mes_referencia"),
                }
            )

    tmp = out_path.parent / "_col_clean_tmp.json"
    try:
        tmp.write_text(json.dumps(rows, ensure_ascii=False), encoding="utf-8")
        out = out_path.as_posix()
        tmp_path = tmp.as_posix()
        con.execute(
            f"COPY (SELECT * FROM read_json_auto('{tmp_path}')) "
            f"TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)"
        )
        return len(rows)
    except Exception as exc:
        raise RuntimeError(f"DuckDB failed writing cost_of_living parquet: {exc}") from exc
    finally:
        try:
            tmp.unlink(missing_ok=True)
        except OSError:
            pass

# -----------------------------------------------------------------------------
# Main pipeline
# -----------------------------------------------------------------------------


def main() -> int:
    load_env()

    try:
        adzuna_path = resolve_latest_adzuna_file()
        year_p, month_p, day_p = partition_segments_from_bronze_path(adzuna_path)
        jooble_path = resolve_jooble_file_for_partition(year_p, month_p, day_p)
        jobbank_path = resolve_jobbank_file_for_partition(year_p, month_p, day_p)
        col_path = resolve_cost_of_living_file(adzuna_path.parent)

        logger.info("Bronze Adzuna: %s", adzuna_path)
        logger.info("Bronze Jooble: %s", jooble_path or "(not available)")
        logger.info("Bronze Job Bank: %s", jobbank_path or "(not available)")
        logger.info("Bronze CoL: %s", col_path)
        logger.info("Silver partition: %s/%s/%s", year_p, month_p, day_p)

        con = duckdb.connect(database=":memory:")

        # --- Cache-Aside geo resolution ---
        geo_cache = load_geo_cache()
        run_id = os.getenv("CANADAPT_RUN_ID", "").strip() or "local-manual"
        adzuna_rows = normalize_adzuna_rows(adzuna_path, run_id)
        jooble_rows = normalize_jooble_rows(jooble_path, run_id)
        jobbank_rows = normalize_jobbank_rows(jobbank_path, run_id)
        job_rows = merge_and_dedupe_job_rows(adzuna_rows, jooble_rows, jobbank_rows)
        distinct_terms = sorted(
            {
                _clean_text(row.get("location_raw"))
                for row in job_rows
                if _clean_text(row.get("location_raw"))
            }
        )
        total_unique = len(distinct_terms)

        required_geo_fields = {"cidade", "provincia", "cma", "metodo", "confianca"}
        missing = [
            term
            for term in distinct_terms
            if term not in geo_cache
            or not required_geo_fields.issubset(geo_cache[term])
        ]
        cache_hits = total_unique - len(missing)

        model_used = ""
        if missing:
            logger.info(
                "Geo cache miss | sending %d new terms to Gemini (batch_size=%d)",
                len(missing),
                GEO_BATCH_SIZE,
            )
            new_mappings, model_used = gemini_map_locations(missing)
            geo_cache.update(new_mappings)
            save_geo_cache(geo_cache)
        else:
            logger.info("Geo cache full hit | API cost $0.00 for geography")
        geo_degraded = any(
            (geo_cache.get(term) or {}).get("metodo") == "gemini_unavailable"
            for term in missing
        )

        # --- Silver jobs ---
        jobs_local = (
            SILVER_ROOT / "jobs" / year_p / month_p / day_p / "jobs_clean.parquet"
        )
        jobs_s3_key = f"silver/jobs/{year_p}/{month_p}/{day_p}/jobs_clean.parquet"
        jobs_n = build_multisource_jobs_clean_parquet(
            con, job_rows, geo_cache, jobs_local
        )
        logger.info("Silver jobs local written | rows=%d | %s", jobs_n, jobs_local)
        logger.info(
            "Silver source mix | adzuna=%d | jooble=%d | jobbank=%d | after_dedupe=%d",
            len(adzuna_rows),
            len(jooble_rows),
            len(jobbank_rows),
            len(job_rows),
        )
        jobs_s3 = upload_file_to_s3(jobs_local, jobs_s3_key)

        # --- Silver cost of living ---
        col_local = (
            SILVER_ROOT
            / "cost_of_living"
            / year_p
            / month_p
            / day_p
            / "cost_of_living_clean.parquet"
        )
        col_s3_key = (
            f"silver/cost_of_living/{year_p}/{month_p}/{day_p}/cost_of_living_clean.parquet"
        )
        col_n = build_cost_of_living_clean_parquet(con, col_path, col_local)
        logger.info("Silver CoL local written | rows=%d | %s", col_n, col_local)
        col_s3 = upload_file_to_s3(col_local, col_s3_key)
        geo_cache_s3 = upload_file_to_s3(
            GEO_CACHE_PATH,
            "silver/metadata/geo_cache.json",
        )

        con.close()

        # --- Metrics ---
        print("\n========== CanAdapt Silver — Resumo ==========")
        print(f"Localizações únicas (Bronze):     {total_unique}")
        print(f"Resolvidas via cache ($0.00):     {cache_hits}")
        print(f"Novas enviadas ao Gemini:         {len(missing)}")
        print(
            f"Modelo Gemini com sucesso:        {model_used or '(nenhuma chamada — 100% cache)'}"
        )
        print(f"Jobs Silver rows:                 {jobs_n}")
        print(f"Cost-of-living Silver rows:       {col_n}")
        print(f"Jobs local:                       {jobs_local}")
        print(f"Jobs S3:                          {jobs_s3}")
        print(f"CoL local:                        {col_local}")
        print(f"CoL S3:                           {col_s3}")
        print(f"Geo cache:                        {GEO_CACHE_PATH}")
        print(f"Geo cache S3:                     {geo_cache_s3}")
        print(f"Contrato de dados:                {DATA_CONTRACT_VERSION}")
        print(f"Prompt geo:                       {GEO_PROMPT_VERSION}")
        print("================================================\n")
        write_step_metrics(
            "process_silver",
            {
                "status": "degraded" if geo_degraded else "success",
                "degraded": geo_degraded,
                "rows": jobs_n,
                "cache_hits": cache_hits,
                "gemini_calls": len(missing),
                "model": model_used or "cache-only",
                "geo_prompt_version": GEO_PROMPT_VERSION,
            },
        )
        return 0

    except (EnvironmentError, FileNotFoundError, RuntimeError, OSError) as exc:
        logger.error("Silver processing failed: %s", exc)
        return 1
    except Exception as exc:  # noqa: BLE001
        logger.exception("Unexpected Silver processing failure: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
