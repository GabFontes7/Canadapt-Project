"""Enrich Silver jobs with auditable salary research (Gemini + Google Search).

Only fills gaps when the posting has no usable declared salary.
A number without a verifiable https source is rejected.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import duckdb
from google import genai
from google.genai import types
from pydantic import BaseModel, Field, ValidationError, field_validator

from pipeline_policy import (
    SALARY_RESEARCH_PROMPT_VERSION as PROMPT_VERSION,
    allow_gemini_degraded,
    gemini_exception_types,
    write_step_metrics,
)
from process_silver import (
    GEMINI_MODEL_FALLBACK_QUEUE,
    load_env,
    require_gemini_api_key,
    upload_file_to_s3,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
CACHE_PATH = SILVER_ROOT / "metadata" / "salary_research_cache.json"
DEFAULT_MAX_JOBS = 25
GEMINI_CATCH = gemini_exception_types()
MIN_ANNUAL = 20_000.0
MAX_ANNUAL = 500_000.0
HTTPS_URL_RE = re.compile(r"https://[^\s\]\)\"'<>]+", re.IGNORECASE)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.processing.salary_research")

SYSTEM_RESEARCH = """
You research Canadian job compensation with Google Search.
Prefer the employer careers page or another posting of the SAME job.
Never invent a salary.
For EVERY job where you find a number, you MUST include at least one line:
SOURCE_URL: https://...
If you cannot provide an https SOURCE_URL, say that no verifiable source was found.
Always note currency and whether the figure is hourly, monthly or annual.
"""

SYSTEM_STRUCTURE = """
Extract only evidence present in the research notes and the provided grounding
source list. Do not invent numbers or URLs.
source_url MUST be an https URL copied from RESEARCH_NOTES or GROUNDING_SOURCES.
If no https URL is available for a job, set found=false.
"""


class SalaryResearchResult(BaseModel):
    research_id: str
    found: bool
    salary_annual_cad_min: float | None = None
    salary_annual_cad_mid: float | None = None
    salary_annual_cad_max: float | None = None
    period_original: str = Field(
        description="hourly, monthly, annual, or unknown"
    )
    currency: str = "CAD"
    source_type: str = Field(
        description="same_job_posting, company_careers, reputable_aggregator, or none"
    )
    source_url: str | None = None
    source_title: str | None = None
    observed_date: str | None = Field(
        default=None, description="YYYY-MM-DD if known"
    )
    confidence: float = Field(ge=0, le=1)
    evidence_summary: str = Field(max_length=400)
    rejection_reason: str | None = None

    @field_validator("period_original")
    @classmethod
    def _period(cls, value: str) -> str:
        normalized = (value or "unknown").strip().lower()
        if normalized not in {"hourly", "monthly", "annual", "unknown"}:
            return "unknown"
        return normalized

    @field_validator("source_type")
    @classmethod
    def _source_type(cls, value: str) -> str:
        normalized = (value or "none").strip().lower()
        allowed = {
            "same_job_posting",
            "company_careers",
            "reputable_aggregator",
            "none",
        }
        return normalized if normalized in allowed else "none"


class SalaryResearchPayload(BaseModel):
    results: list[SalaryResearchResult]


def _partition_tuple(path: Path) -> tuple[int, int, int]:
    values: dict[str, int] = {}
    for part in path.parts:
        if "=" in part:
            key, value = part.split("=", 1)
            if key in {"year", "month", "day"}:
                values[key] = int(value)
    return values.get("year", 0), values.get("month", 0), values.get("day", 0)


def _latest_jobs_path() -> Path:
    paths = list((SILVER_ROOT / "jobs").glob("year=*/month=*/day=*/jobs_clean.parquet"))
    if not paths:
        raise FileNotFoundError("No Silver jobs_clean.parquet partition found")
    return max(paths, key=_partition_tuple)


def _cache_key(title: str, company: str, city: str, province: str) -> str:
    raw = "|".join(
        re.sub(r"\s+", " ", (value or "").strip().lower())
        for value in (title, company, city, province)
    )
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


def _load_cache() -> dict[str, dict[str, Any]]:
    if not CACHE_PATH.exists():
        return {}
    return json.loads(CACHE_PATH.read_text(encoding="utf-8"))


def _save_cache(cache: dict[str, dict[str, Any]]) -> None:
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    CACHE_PATH.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )


def _normalize_model_name(model: str) -> str:
    name = model.strip()
    return name[len("models/") :] if name.startswith("models/") else name


def _valid_https_url(url: str | None) -> bool:
    if not url:
        return False
    cleaned = url.strip().rstrip(".,);]")
    parsed = urlparse(cleaned)
    return parsed.scheme == "https" and bool(parsed.netloc)


def _extract_urls_from_text(text: str) -> list[dict[str, str]]:
    found: list[dict[str, str]] = []
    seen: set[str] = set()
    for match in HTTPS_URL_RE.findall(text or ""):
        url = match.rstrip(".,);]")
        if not _valid_https_url(url) or url in seen:
            continue
        seen.add(url)
        found.append({"url": url, "title": urlparse(url).netloc})
    return found


def _extract_grounding_sources(response: Any) -> list[dict[str, str]]:
    sources: list[dict[str, str]] = []
    seen: set[str] = set()
    try:
        candidates = getattr(response, "candidates", None) or []
        for candidate in candidates:
            metadata = getattr(candidate, "grounding_metadata", None)
            if not metadata:
                continue
            chunks = getattr(metadata, "grounding_chunks", None) or []
            for chunk in chunks:
                web = getattr(chunk, "web", None)
                if not web:
                    continue
                uri = getattr(web, "uri", None) or getattr(web, "url", None)
                title = getattr(web, "title", None) or ""
                if not _valid_https_url(uri):
                    continue
                uri = str(uri).strip()
                if uri in seen:
                    continue
                seen.add(uri)
                sources.append({"url": uri, "title": str(title)})
    except Exception as exc:  # noqa: BLE001
        logger.warning("Could not parse grounding metadata: %s", exc)
    return sources


def _prefer_source_url(
    candidate_url: str | None, grounding_sources: list[dict[str, str]]
) -> tuple[str | None, str | None]:
    if _valid_https_url(candidate_url):
        title = next(
            (
                item["title"]
                for item in grounding_sources
                if item["url"] == candidate_url
            ),
            urlparse(candidate_url.strip()).netloc,
        )
        return candidate_url.strip().rstrip(".,);]"), title

    preferred_tokens = (
        "career",
        "jobs",
        "lever.co",
        "greenhouse.io",
        "workday",
        "linkedin.com",
        "indeed.com",
        "adzuna",
        "glassdoor",
    )
    ranked = sorted(
        grounding_sources,
        key=lambda item: (
            0
            if any(token in item["url"].lower() or token in item["title"].lower()
                   for token in preferred_tokens)
            else 1,
            item["title"],
        ),
    )
    if ranked:
        return ranked[0]["url"], ranked[0]["title"]
    return None, None


def _annualize(value: float, period: str) -> float | None:
    if period == "annual":
        return value
    if period == "monthly":
        return value * 12
    if period == "hourly":
        return value * 2080
    return None


def _accept_result(
    item: SalaryResearchResult,
    grounding_sources: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    grounding_sources = grounding_sources or []
    source_url, source_title = _prefer_source_url(item.source_url, grounding_sources)

    # Recover missing URL from grounding when a midpoint/range exists.
    has_number = item.salary_annual_cad_mid is not None or (
        item.salary_annual_cad_min is not None
        and item.salary_annual_cad_max is not None
    )
    if has_number and source_url is not None:
        item = item.model_copy(
            update={
                "found": True,
                "source_url": source_url,
                "source_title": source_title or item.source_title,
                "source_type": item.source_type
                if item.source_type != "none"
                else "reputable_aggregator",
                "confidence": max(float(item.confidence or 0), 0.6),
                "period_original": item.period_original
                if item.period_original != "unknown"
                else "annual",
                "salary_annual_cad_mid": item.salary_annual_cad_mid
                if item.salary_annual_cad_mid is not None
                else (
                    (item.salary_annual_cad_min + item.salary_annual_cad_max) / 2
                    if item.salary_annual_cad_min is not None
                    and item.salary_annual_cad_max is not None
                    else None
                ),
            }
        )
    elif source_url and not item.source_url:
        item = item.model_copy(
            update={
                "source_url": source_url,
                "source_title": source_title or item.source_title,
            }
        )

    base = {
        "research_id": item.research_id,
        "found": False,
        "salary_research_annual_min": None,
        "salary_research_annual_mid": None,
        "salary_research_annual_max": None,
        "salary_research_period_original": item.period_original,
        "salary_research_currency": (item.currency or "").upper() or None,
        "salary_research_source_type": item.source_type,
        "salary_research_source_url": item.source_url,
        "salary_research_source_title": item.source_title,
        "salary_research_observed_date": item.observed_date,
        "salary_research_confidence": item.confidence,
        "salary_research_evidence": item.evidence_summary,
        "salary_research_rejection_reason": item.rejection_reason,
        "salary_research_prompt_version": PROMPT_VERSION,
        "salary_research_at_utc": datetime.now(timezone.utc).isoformat(),
        "salary_research_grounding_urls": [s["url"] for s in grounding_sources],
    }
    if not item.found:
        base["salary_research_rejection_reason"] = (
            item.rejection_reason or "no_verifiable_source"
        )
        return base

    if (item.currency or "").upper() not in {"CAD", "CDN", "C$"}:
        # Default CAD for Canadian postings when model omitted currency but
        # returned an otherwise valid grounded salary.
        if item.currency in (None, "", "unknown"):
            item = item.model_copy(update={"currency": "CAD"})
            base["salary_research_currency"] = "CAD"
        else:
            base["salary_research_rejection_reason"] = "currency_not_cad"
            return base
    if not _valid_https_url(item.source_url):
        base["salary_research_rejection_reason"] = "missing_https_source_url"
        return base
    if item.source_type == "none":
        base["salary_research_rejection_reason"] = "source_type_none"
        return base
    if item.period_original == "unknown":
        base["salary_research_rejection_reason"] = "period_unknown"
        return base
    if item.confidence < 0.55:
        base["salary_research_rejection_reason"] = "confidence_too_low"
        return base

    mid = item.salary_annual_cad_mid
    low = item.salary_annual_cad_min
    high = item.salary_annual_cad_max
    if mid is not None and item.period_original in {"hourly", "monthly"} and mid < 1000:
        mid = _annualize(mid, item.period_original)
        low = _annualize(low, item.period_original) if low is not None else None
        high = _annualize(high, item.period_original) if high is not None else None
    if mid is None and low is not None and high is not None:
        mid = (low + high) / 2
    if mid is None:
        base["salary_research_rejection_reason"] = "missing_midpoint"
        return base
    if not (MIN_ANNUAL <= mid <= MAX_ANNUAL):
        base["salary_research_rejection_reason"] = "annual_out_of_range"
        return base
    if low is not None and high is not None and high < low:
        base["salary_research_rejection_reason"] = "range_inverted"
        return base

    base.update(
        {
            "found": True,
            "salary_research_annual_min": round(low, 2) if low is not None else None,
            "salary_research_annual_mid": round(mid, 2),
            "salary_research_annual_max": round(high, 2) if high is not None else None,
            "salary_research_rejection_reason": None,
            "salary_research_source_url": item.source_url,
            "salary_research_source_title": item.source_title,
            "salary_research_source_type": item.source_type,
            "salary_research_currency": (item.currency or "CAD").upper(),
        }
    )
    return base


def _gemini_research_text(
    prompt: str,
) -> tuple[str, list[dict[str, str]], str]:
    client = genai.Client(api_key=require_gemini_api_key())
    errors: list[str] = []
    for model in GEMINI_MODEL_FALLBACK_QUEUE:
        model_id = _normalize_model_name(model)
        try:
            response = client.models.generate_content(
                model=model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_RESEARCH,
                    tools=[types.Tool(google_search=types.GoogleSearch())],
                    temperature=0.2,
                ),
            )
            text = (response.text or "").strip()
            if not text:
                raise RuntimeError("empty research text")
            grounding = _extract_grounding_sources(response)
            from_text = _extract_urls_from_text(text)
            merged: list[dict[str, str]] = []
            seen: set[str] = set()
            for item in grounding + from_text:
                if item["url"] in seen:
                    continue
                seen.add(item["url"])
                merged.append(item)
            logger.info(
                "Salary research sources | model=%s | grounding=%d | text_urls=%d",
                model_id,
                len(grounding),
                len(from_text),
            )
            return text, merged, model_id
        except GEMINI_CATCH as exc:
            errors.append(f"{model_id}: {exc}")
            logger.warning("Salary research text failed | model=%s | %s", model_id, exc)
            time.sleep(1.5)
    raise RuntimeError("All Gemini research models failed: " + " | ".join(errors))


def _gemini_structure(prompt: str) -> tuple[SalaryResearchPayload, str]:
    client = genai.Client(api_key=require_gemini_api_key())
    errors: list[str] = []
    for model in GEMINI_MODEL_FALLBACK_QUEUE:
        model_id = _normalize_model_name(model)
        try:
            response = client.models.generate_content(
                model=model_id,
                contents=prompt,
                config=types.GenerateContentConfig(
                    system_instruction=SYSTEM_STRUCTURE,
                    response_mime_type="application/json",
                    response_schema=SalaryResearchPayload,
                    temperature=0.1,
                ),
            )
            raw = (response.text or "").strip()
            if not raw:
                raise RuntimeError("empty structured response")
            return SalaryResearchPayload.model_validate_json(raw), model_id
        except GEMINI_CATCH + (ValidationError, ValueError) as exc:
            errors.append(f"{model_id}: {exc}")
            logger.warning(
                "Salary structure failed | model=%s | %s", model_id, exc
            )
            time.sleep(1.0)
    raise RuntimeError("All Gemini structure models failed: " + " | ".join(errors))


def _jobs_needing_research(
    con: duckdb.DuckDBPyConnection, jobs_path: Path
) -> list[dict[str, Any]]:
    rows = con.execute(
        """
        select
            job_id,
            coalesce(title, '') as title,
            coalesce(company, '') as company,
            coalesce(cidade_padronizada, '') as city,
            coalesce(provincia_padronizada, '') as province,
            coalesce(redirect_url, '') as url_vaga,
            substr(coalesce(description, ''), 1, 500) as description_excerpt
        from read_parquet(?)
        where (
            salary_min is null
            or coalesce(salary_is_predicted, false) = true
            or salary_min < 20000
            or salary_min > 500000
        )
        order by created desc nulls last
        """,
        [str(jobs_path)],
    ).fetchdf()
    jobs: list[dict[str, Any]] = []
    for record in rows.to_dict(orient="records"):
        research_id = _cache_key(
            record["title"],
            record["company"],
            record["city"],
            record["province"],
        )
        jobs.append({**record, "research_id": research_id})
    return jobs


def _research_batch(batch: list[dict[str, Any]]) -> tuple[dict[str, dict], str]:
    listing = []
    for item in batch:
        listing.append(
            {
                "research_id": item["research_id"],
                "title": item["title"],
                "company": item["company"],
                "city": item["city"],
                "province": item["province"],
                "job_url": item["url_vaga"],
                "description_excerpt": item["description_excerpt"],
            }
        )
    research_prompt = (
        "Search the public web for compensation evidence for each Canadian job "
        "below. Prefer the employer careers page or another posting of the same "
        "role. For each research_id with a salary finding, include an explicit "
        "line `SOURCE_URL: https://...` plus currency, period and amount.\n\n"
        f"JOBS_JSON:\n{json.dumps(listing, ensure_ascii=False)}"
    )
    notes, grounding_sources, research_model = _gemini_research_text(research_prompt)
    structure_prompt = (
        "Convert the research notes into the JSON schema. Reproduce each "
        "research_id exactly. source_url MUST be copied from RESEARCH_NOTES "
        "or GROUNDING_SOURCES and must start with https://. "
        "If a job has salary numbers but no https URL in those lists, "
        "set found=false.\n\n"
        f"RESEARCH_NOTES:\n{notes}\n\n"
        f"GROUNDING_SOURCES:\n{json.dumps(grounding_sources, ensure_ascii=False)}\n\n"
        f"EXPECTED_IDS:\n{json.dumps([i['research_id'] for i in batch])}"
    )
    payload, structure_model = _gemini_structure(structure_prompt)
    model_used = f"{research_model}+{structure_model}"
    accepted: dict[str, dict] = {}
    requested = {item["research_id"] for item in batch}
    for result in payload.results:
        if result.research_id not in requested:
            continue
        row = _accept_result(result, grounding_sources)
        row["salary_research_model"] = model_used
        accepted[result.research_id] = row
    for item in batch:
        if item["research_id"] not in accepted:
            accepted[item["research_id"]] = {
                "research_id": item["research_id"],
                "found": False,
                "salary_research_annual_min": None,
                "salary_research_annual_mid": None,
                "salary_research_annual_max": None,
                "salary_research_period_original": "unknown",
                "salary_research_currency": None,
                "salary_research_source_type": "none",
                "salary_research_source_url": None,
                "salary_research_source_title": None,
                "salary_research_observed_date": None,
                "salary_research_confidence": 0.0,
                "salary_research_evidence": "No structured result returned",
                "salary_research_rejection_reason": "missing_from_model_response",
                "salary_research_prompt_version": PROMPT_VERSION,
                "salary_research_model": model_used,
                "salary_research_at_utc": datetime.now(timezone.utc).isoformat(),
                "salary_research_grounding_urls": [
                    source["url"] for source in grounding_sources
                ],
            }
    return accepted, model_used


def _rewrite_jobs(
    con: duckdb.DuckDBPyConnection,
    jobs_path: Path,
    cache: dict[str, dict[str, Any]],
) -> int:
    map_path = jobs_path.parent / ".salary_research_map.tmp.json"
    out_path = jobs_path.parent / ".jobs_clean.salary_research.tmp.parquet"
    mapping_rows = []
    for research_id, values in cache.items():
        mapping_rows.append(
            {
                "research_id": research_id,
                "salary_research_found": bool(values.get("found")),
                "salary_research_annual_min": values.get("salary_research_annual_min"),
                "salary_research_annual_mid": values.get("salary_research_annual_mid"),
                "salary_research_annual_max": values.get("salary_research_annual_max"),
                "salary_research_period_original": values.get(
                    "salary_research_period_original"
                ),
                "salary_research_currency": values.get("salary_research_currency"),
                "salary_research_source_type": values.get(
                    "salary_research_source_type"
                ),
                "salary_research_source_url": values.get("salary_research_source_url"),
                "salary_research_source_title": values.get(
                    "salary_research_source_title"
                ),
                "salary_research_observed_date": values.get(
                    "salary_research_observed_date"
                ),
                "salary_research_confidence": values.get("salary_research_confidence"),
                "salary_research_evidence": values.get("salary_research_evidence"),
                "salary_research_rejection_reason": values.get(
                    "salary_research_rejection_reason"
                ),
                "salary_research_model": values.get("salary_research_model"),
                "salary_research_prompt_version": values.get(
                    "salary_research_prompt_version", PROMPT_VERSION
                ),
                "salary_research_at_utc": values.get("salary_research_at_utc"),
            }
        )
    map_path.write_text(json.dumps(mapping_rows, ensure_ascii=False), encoding="utf-8")
    enrichment_columns = {
        "salary_research_found",
        "salary_research_annual_min",
        "salary_research_annual_mid",
        "salary_research_annual_max",
        "salary_research_period_original",
        "salary_research_currency",
        "salary_research_source_type",
        "salary_research_source_url",
        "salary_research_source_title",
        "salary_research_observed_date",
        "salary_research_confidence",
        "salary_research_evidence",
        "salary_research_rejection_reason",
        "salary_research_model",
        "salary_research_prompt_version",
        "salary_research_at_utc",
        "salary_research_cache_key",
    }
    try:
        source_columns = [
            row[0]
            for row in con.execute(
                "describe select * from read_parquet(?)", [str(jobs_path)]
            ).fetchall()
            if row[0] not in enrichment_columns
        ]
        projected = ",\n                    ".join(
            f'j."{column.replace(chr(34), chr(34) * 2)}"'
            for column in source_columns
        )
        jobs_sql = jobs_path.as_posix().replace("'", "''")
        map_sql = map_path.as_posix().replace("'", "''")
        out_sql = out_path.as_posix().replace("'", "''")
        con.execute(
            f"""
            COPY (
                select
                    {projected},
                    m.salary_research_found,
                    m.salary_research_annual_min,
                    m.salary_research_annual_mid,
                    m.salary_research_annual_max,
                    m.salary_research_period_original,
                    m.salary_research_currency,
                    m.salary_research_source_type,
                    m.salary_research_source_url,
                    m.salary_research_source_title,
                    m.salary_research_observed_date,
                    m.salary_research_confidence,
                    m.salary_research_evidence,
                    m.salary_research_rejection_reason,
                    m.salary_research_model,
                    m.salary_research_prompt_version,
                    try_cast(m.salary_research_at_utc as timestamp)
                        as salary_research_at_utc,
                    m.research_id as salary_research_cache_key
                from read_parquet('{jobs_sql}') j
                left join read_json_auto('{map_sql}') m
                    on m.research_id = sha256(
                        regexp_replace(lower(trim(coalesce(j.title, ''))), '[[:space:]]+', ' ', 'g')
                        || '|'
                        || regexp_replace(lower(trim(coalesce(j.company, ''))), '[[:space:]]+', ' ', 'g')
                        || '|'
                        || regexp_replace(
                            lower(trim(coalesce(j.cidade_padronizada, ''))),
                            '[[:space:]]+',
                            ' ',
                            'g'
                        )
                        || '|'
                        || regexp_replace(
                            lower(trim(coalesce(j.provincia_padronizada, ''))),
                            '[[:space:]]+',
                            ' ',
                            'g'
                        )
                    )
            ) TO '{out_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """
        )
        rows = con.execute(
            "select count(*) from read_parquet(?)", [str(out_path)]
        ).fetchone()[0]
    finally:
        map_path.unlink(missing_ok=True)
    out_path.replace(jobs_path)
    return int(rows)


def main() -> int:
    try:
        load_env()
        max_jobs = int(os.getenv("SALARY_RESEARCH_MAX_JOBS", str(DEFAULT_MAX_JOBS)))
        jobs_path = _latest_jobs_path()
        cache = _load_cache()
        with duckdb.connect(":memory:") as con:
            candidates = _jobs_needing_research(con, jobs_path)
            todo = [
                job
                for job in candidates
                if (
                    job["research_id"] not in cache
                    or cache[job["research_id"]].get("salary_research_prompt_version")
                    != PROMPT_VERSION
                    or cache[job["research_id"]].get("salary_research_rejection_reason")
                    == "not_researched_yet"
                )
            ][:max_jobs]
            logger.info(
                "Salary research queue | candidates=%d | todo=%d | max=%d",
                len(candidates),
                len(todo),
                max_jobs,
            )
            model_used = "cache-only"
            new_count = 0
            found_count = 0
            salary_degraded = False
            # Small batches keep grounding prompts focused.
            batch_size = 3
            for start in range(0, len(todo), batch_size):
                batch = todo[start : start + batch_size]
                try:
                    mapped, model_used = _research_batch(batch)
                except GEMINI_CATCH as exc:
                    if not allow_gemini_degraded():
                        raise
                    salary_degraded = True
                    logger.warning("Salary research batch skipped | %s", exc)
                    continue
                cache.update(mapped)
                new_count += len(mapped)
                found_count += sum(1 for row in mapped.values() if row.get("found"))
                logger.info(
                    "Salary research progress | batch_found=%d | total_found=%d",
                    sum(1 for row in mapped.values() if row.get("found")),
                    found_count,
                )
                _save_cache(cache)
                time.sleep(1.0)

            # Ensure every candidate has a cache stub for rewrite joins.
            for job in candidates:
                cache.setdefault(
                    job["research_id"],
                    {
                        "research_id": job["research_id"],
                        "found": False,
                        "salary_research_annual_mid": None,
                        "salary_research_prompt_version": PROMPT_VERSION,
                        "salary_research_rejection_reason": "not_researched_yet",
                        "salary_research_at_utc": datetime.now(
                            timezone.utc
                        ).isoformat(),
                    },
                )
            _save_cache(cache)
            rows = _rewrite_jobs(con, jobs_path, cache)
            if todo and new_count == 0:
                salary_degraded = True

        year, month, day = _partition_tuple(jobs_path)
        jobs_key = (
            f"silver/jobs/year={year}/month={month:02d}/day={day:02d}/"
            "jobs_clean.parquet"
        )
        jobs_s3 = upload_file_to_s3(jobs_path, jobs_key)
        cache_s3 = upload_file_to_s3(
            CACHE_PATH, "silver/metadata/salary_research_cache.json"
        )
        logger.info(
            "Salary research complete | rows=%d | researched=%d | found=%d | "
            "model=%s | degraded=%s | jobs_s3=%s | cache_s3=%s",
            rows,
            new_count,
            found_count,
            model_used,
            salary_degraded,
            jobs_s3,
            cache_s3,
        )
        write_step_metrics(
            "enrich_salary_research",
            {
                "status": "degraded" if salary_degraded else "success",
                "degraded": salary_degraded,
                "rows": rows,
                "gemini_calls": new_count,
                "found": found_count,
                "queued": len(todo),
                "model": model_used,
                "salary_research_prompt_version": PROMPT_VERSION,
            },
        )
        return 0
    except (duckdb.Error, OSError, RuntimeError, ValueError, EnvironmentError) as exc:
        logger.error("Salary research failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
