"""Enrich the latest Silver job partition with NOC and seniority metadata."""

from __future__ import annotations

import json
import logging
import hashlib
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb
from pydantic import BaseModel, Field

from pipeline_policy import (
    NOC_PROMPT_VERSION,
    allow_gemini_degraded,
    write_step_metrics,
)
from process_silver import call_gemini_with_fallback, load_env, upload_file_to_s3

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
NOC_CACHE_PATH = SILVER_ROOT / "metadata" / "noc_cache.json"
NOC_BATCH_SIZE = 25
NOC_CACHE_VERSION = 2
NOC_RECLASSIFY_BELOW_CONFIDENCE = 0.65

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.processing.enrich_jobs")

NOC_SYSTEM_INSTRUCTION = """
Você é um classificador especialista na National Occupational Classification
(NOC) 2021 do Canadá. Para cada vaga, escolha exatamente um código NOC de
cinco dígitos com base no título, categoria e trecho da descrição. Não invente
códigos. Retorne também o título oficial em inglês, senioridade entre
entry, junior, mid, senior, lead, manager, director, executive ou unknown,
e confiança entre 0 e 1. O context_id deve ser reproduzido sem alterações.
Em evidência, explique em no máximo 160 caracteres quais tarefas da descrição
sustentam o NOC escolhido. Se o contexto for ambíguo, reduza a confiança.
"""


class NocMapping(BaseModel):
    context_id: str
    noc_code: str = Field(description="Código NOC 2021 com cinco dígitos")
    noc_title: str
    seniority: str
    confidence: float = Field(ge=0, le=1)
    evidence: str = Field(max_length=200)


class NocMappingPayload(BaseModel):
    mapeamentos: list[NocMapping]


def _partition_tuple(path: Path) -> tuple[int, int, int]:
    values = {}
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


def _latest_wages_path() -> Path:
    paths = list(
        (SILVER_ROOT / "wages").glob(
            "reference_year=*/wages_official.parquet"
        )
    )
    if not paths:
        raise FileNotFoundError("No Silver official wage benchmark found")
    return max(
        paths,
        key=lambda p: int(
            next(
                part.split("=", 1)[1]
                for part in p.parts
                if part.startswith("reference_year=")
            )
        ),
    )


def _load_cache() -> tuple[dict[str, dict], dict[str, dict]]:
    NOC_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not NOC_CACHE_PATH.exists():
        return {}, {}
    payload = json.loads(NOC_CACHE_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("NOC cache must be a JSON object")
    if payload.get("version") == NOC_CACHE_VERSION:
        entries = payload.get("entries", {})
        if not isinstance(entries, dict):
            raise RuntimeError("NOC v2 cache entries must be a JSON object")
        return entries, {}
    logger.warning("Legacy title-only NOC cache detected; migrating high-confidence entries")
    return {}, payload


def _save_cache(cache: dict[str, dict]) -> None:
    tmp = NOC_CACHE_PATH.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(
            {
                "version": NOC_CACHE_VERSION,
                "prompt_version": NOC_PROMPT_VERSION,
                "updated_at_utc": datetime.now(timezone.utc).isoformat(),
                "entries": cache,
            },
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    tmp.replace(NOC_CACHE_PATH)


def _normalize(value: str | None) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _context_id(title: str, category: str | None, description: str | None) -> str:
    canonical = "|".join(
        (_normalize(title), _normalize(category), _normalize(description)[:800])
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _job_contexts(con: duckdb.DuckDBPyConnection, jobs_path: Path) -> list[dict]:
    rows = con.execute(
        """
        select
            title,
            category,
            substr(description, 1, 800) as description
        from read_parquet(?)
        where title is not null and trim(title) <> ''
        group by title, category, description
        order by title, category
        """,
        [str(jobs_path)],
    ).fetchall()
    contexts = []
    for title, category, description in rows:
        contexts.append(
            {
                "context_id": _context_id(title, category, description),
                "title": title,
                "category": category,
                "description": description,
            }
        )
    return contexts


def _valid_noc_codes(
    con: duckdb.DuckDBPyConnection, wages_path: Path
) -> set[str]:
    return {
        row[0]
        for row in con.execute(
            "select distinct noc_code from read_parquet(?) where noc_code is not null",
            [str(wages_path)],
        ).fetchall()
    }


def _map_new_titles(
    contexts: list[dict],
    valid_codes: set[str],
) -> tuple[dict[str, dict], str]:
    mapped: dict[str, dict] = {}
    model_used = ""
    for start in range(0, len(contexts), NOC_BATCH_SIZE):
        batch = contexts[start : start + NOC_BATCH_SIZE]
        prompt = (
            "Classifique as vagas abaixo no contrato NocMappingPayload.\n"
            "Reproduza context_id exatamente como recebido.\n\n"
            + json.dumps(batch, ensure_ascii=False)
        )
        try:
            payload, model_used = call_gemini_with_fallback(
                prompt,
                NocMappingPayload,
                system_instruction=NOC_SYSTEM_INSTRUCTION,
            )
        except RuntimeError as exc:
            if not allow_gemini_degraded():
                raise
            logger.warning(
                "NOC batch skipped (Gemini unavailable) | start=%d | size=%d | %s",
                start,
                len(batch),
                exc,
            )
            continue
        requested = {item["context_id"] for item in batch}
        for item in payload.mapeamentos:
            code = item.noc_code.removeprefix("NOC_").strip().zfill(5)
            if item.context_id not in requested:
                continue
            if code not in valid_codes:
                logger.warning(
                    "NOC lacks official wage benchmark; confidence capped | "
                    "context_id=%s | code=%s",
                    item.context_id,
                    code,
                )
            context = next(
                candidate for candidate in batch
                if candidate["context_id"] == item.context_id
            )
            mapped[item.context_id] = {
                "title": context["title"],
                "category": context["category"],
                "noc_code": code,
                "noc_title": item.noc_title,
                "seniority": item.seniority,
                "noc_confidence": min(item.confidence, 0.5)
                if code not in valid_codes
                else item.confidence,
                "noc_has_wage_benchmark": code in valid_codes,
                "noc_mapping_method": "gemini_context_fingerprint",
                "noc_evidence": item.evidence,
                "noc_model": model_used,
                "noc_prompt_version": NOC_PROMPT_VERSION,
                "noc_classified_at_utc": datetime.now(timezone.utc).isoformat(),
            }
    return mapped, model_used


def _rewrite_jobs(
    con: duckdb.DuckDBPyConnection,
    jobs_path: Path,
    cache: dict[str, dict],
) -> int:
    map_path = jobs_path.parent / ".noc_map.tmp.json"
    out_path = jobs_path.parent / ".jobs_clean.enriched.tmp.parquet"
    mapping_rows = [
        {"context_id": context_id, **values}
        for context_id, values in cache.items()
    ]
    if not mapping_rows:
        raise RuntimeError("No valid NOC mappings available to enrich jobs")
    map_path.write_text(json.dumps(mapping_rows, ensure_ascii=False), encoding="utf-8")
    try:
        enrichment_columns = {
            "noc_code",
            "noc_title",
            "seniority",
            "noc_confidence",
            "noc_mapping_method",
            "noc_cache_key",
            "noc_evidence",
            "noc_model",
            "noc_prompt_version",
            "noc_classified_at_utc",
            "noc_has_wage_benchmark",
        }
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
                    m.noc_code,
                    m.noc_title,
                    m.seniority,
                    m.noc_confidence,
                    m.noc_mapping_method,
                    m.context_id as noc_cache_key,
                    m.noc_evidence,
                    m.noc_model,
                    m.noc_prompt_version,
                    try_cast(m.noc_classified_at_utc as timestamp)
                        as noc_classified_at_utc,
                    m.noc_has_wage_benchmark
                from read_parquet('{jobs_sql}') j
                left join read_json_auto('{map_sql}') m
                    on m.context_id = sha256(
                        regexp_replace(lower(trim(coalesce(j.title, ''))), '[[:space:]]+', ' ', 'g')
                        || '|'
                        || regexp_replace(lower(trim(coalesce(j.category, ''))), '[[:space:]]+', ' ', 'g')
                        || '|'
                        || regexp_replace(
                            lower(trim(substr(coalesce(j.description, ''), 1, 800))),
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
        jobs_path = _latest_jobs_path()
        wages_path = _latest_wages_path()
        cache, legacy_cache = _load_cache()
        with duckdb.connect(":memory:") as con:
            contexts = _job_contexts(con, jobs_path)
            valid_codes = _valid_noc_codes(con, wages_path)
            for cached in cache.values():
                cached.setdefault(
                    "noc_has_wage_benchmark",
                    str(cached.get("noc_code") or "") in valid_codes,
                )
            migrated = 0
            for context in contexts:
                legacy = legacy_cache.get(context["title"])
                if (
                    context["context_id"] not in cache
                    and legacy
                    and float(legacy.get("noc_confidence") or 0) >= 0.85
                ):
                    cache[context["context_id"]] = {
                        **legacy,
                        "title": context["title"],
                        "category": context["category"],
                        "noc_mapping_method": "legacy_title_cache_migrated",
                        "noc_prompt_version": "legacy_title_v1",
                        "noc_evidence": "Migrated high-confidence title-only mapping",
                        "noc_model": "legacy",
                        "noc_classified_at_utc": datetime.now(timezone.utc).isoformat(),
                        "noc_has_wage_benchmark": str(legacy.get("noc_code")) in valid_codes,
                    }
                    migrated += 1
            missing = [
                item
                for item in contexts
                if item["context_id"] not in cache
                or cache[item["context_id"]].get("noc_prompt_version")
                != NOC_PROMPT_VERSION
                or float(cache[item["context_id"]].get("noc_confidence") or 0)
                < NOC_RECLASSIFY_BELOW_CONFIDENCE
            ]
            new_mappings, model_used = _map_new_titles(missing, valid_codes)
            cache.update(new_mappings)
            _save_cache(cache)
            rows = _rewrite_jobs(con, jobs_path, cache)
            still_missing = [
                item["context_id"]
                for item in missing
                if item["context_id"] not in cache
            ]
            noc_degraded = bool(still_missing)

        year, month, day = _partition_tuple(jobs_path)
        jobs_key = (
            f"silver/jobs/year={year}/month={month:02d}/day={day:02d}/"
            "jobs_clean.parquet"
        )
        jobs_s3 = upload_file_to_s3(jobs_path, jobs_key)
        cache_s3 = upload_file_to_s3(
            NOC_CACHE_PATH, "silver/metadata/noc_cache.json"
        )
        logger.info(
            "NOC enrichment complete | rows=%d | contexts=%d | cache_hits=%d | "
            "migrated=%d | new=%d | model=%s | degraded=%s | jobs_s3=%s | cache_s3=%s",
            rows,
            len(contexts),
            len(contexts) - len(missing),
            migrated,
            len(new_mappings),
            model_used or "cache-only",
            noc_degraded,
            jobs_s3,
            cache_s3,
        )
        write_step_metrics(
            "enrich_jobs",
            {
                "status": "degraded" if noc_degraded else "success",
                "degraded": noc_degraded,
                "rows": rows,
                "cache_hits": len(contexts) - len(missing),
                "gemini_calls": len(new_mappings),
                "skipped_contexts": len(still_missing),
                "model": model_used or "cache-only",
                "noc_prompt_version": NOC_PROMPT_VERSION,
            },
        )
        return 0
    except (duckdb.Error, OSError, RuntimeError, ValueError) as exc:
        logger.error("NOC enrichment failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
