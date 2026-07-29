"""Enrich the latest Silver job partition with NOC and seniority metadata."""

from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

import duckdb
from pydantic import BaseModel, Field

from process_silver import call_gemini_with_fallback, load_env, upload_file_to_s3

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_ROOT = PROJECT_ROOT / "data" / "silver"
NOC_CACHE_PATH = SILVER_ROOT / "metadata" / "noc_cache.json"
NOC_BATCH_SIZE = 25

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
e confiança entre 0 e 1. O termo_original deve ser reproduzido sem alterações.
"""


class NocMapping(BaseModel):
    termo_original: str
    noc_code: str = Field(description="Código NOC 2021 com cinco dígitos")
    noc_title: str
    seniority: str
    confidence: float = Field(ge=0, le=1)


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


def _load_cache() -> dict[str, dict]:
    NOC_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not NOC_CACHE_PATH.exists():
        return {}
    payload = json.loads(NOC_CACHE_PATH.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise RuntimeError("NOC cache must be a JSON object")
    return payload


def _save_cache(cache: dict[str, dict]) -> None:
    tmp = NOC_CACHE_PATH.with_suffix(".tmp")
    tmp.write_text(
        json.dumps(cache, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    tmp.replace(NOC_CACHE_PATH)


def _job_contexts(con: duckdb.DuckDBPyConnection, jobs_path: Path) -> list[dict]:
    rows = con.execute(
        """
        select
            title,
            any_value(category) as category,
            substr(any_value(description), 1, 400) as description
        from read_parquet(?)
        where title is not null and trim(title) <> ''
        group by title
        order by title
        """,
        [str(jobs_path)],
    ).fetchall()
    return [
        {"title": title, "category": category, "description": description}
        for title, category, description in rows
    ]


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
            "Use o campo title exatamente como termo_original.\n\n"
            + json.dumps(batch, ensure_ascii=False)
        )
        try:
            payload, model_used = call_gemini_with_fallback(
                prompt,
                NocMappingPayload,
                system_instruction=NOC_SYSTEM_INSTRUCTION,
            )
        except RuntimeError as exc:
            logger.error(
                "NOC batch failed after all fallbacks | start=%d | size=%d | %s",
                start,
                len(batch),
                exc,
            )
            continue
        requested = {item["title"] for item in batch}
        for item in payload.mapeamentos:
            code = item.noc_code.removeprefix("NOC_").strip()
            if item.termo_original not in requested:
                continue
            if code not in valid_codes:
                logger.warning(
                    "Rejected unknown NOC code | title=%r | code=%s",
                    item.termo_original,
                    code,
                )
                continue
            mapped[item.termo_original] = {
                "noc_code": code,
                "noc_title": item.noc_title,
                "seniority": item.seniority,
                "noc_confidence": item.confidence,
                "noc_mapping_method": "gemini_title_description",
            }
    return mapped, model_used


def _rewrite_jobs(
    con: duckdb.DuckDBPyConnection,
    jobs_path: Path,
    cache: dict[str, dict],
) -> int:
    map_path = jobs_path.parent / ".noc_map.tmp.json"
    out_path = jobs_path.parent / ".jobs_clean.enriched.tmp.parquet"
    mapping_rows = [{"title": title, **values} for title, values in cache.items()]
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
                    m.noc_mapping_method
                from read_parquet('{jobs_sql}') j
                left join read_json_auto('{map_sql}') m using (title)
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
        cache = _load_cache()
        with duckdb.connect(":memory:") as con:
            contexts = _job_contexts(con, jobs_path)
            valid_codes = _valid_noc_codes(con, wages_path)
            missing = [item for item in contexts if item["title"] not in cache]
            new_mappings, model_used = _map_new_titles(missing, valid_codes)
            cache.update(new_mappings)
            _save_cache(cache)
            rows = _rewrite_jobs(con, jobs_path, cache)

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
            "NOC enrichment complete | rows=%d | titles=%d | cache_hits=%d | "
            "new=%d | model=%s | jobs_s3=%s | cache_s3=%s",
            rows,
            len(contexts),
            len(contexts) - len(missing),
            len(new_mappings),
            model_used or "cache-only",
            jobs_s3,
            cache_s3,
        )
        return 0
    except (duckdb.Error, OSError, RuntimeError, ValueError) as exc:
        logger.error("NOC enrichment failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
