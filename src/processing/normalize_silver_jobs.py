"""Rewrite Silver job partitions so mixed Parquet types union in DuckDB/dbt.

Older files stored pipeline_run_id as UUID; newer GHA runs use VARCHAR
('gha-123-1'). A glob read then fails with a UUID cast error.
"""

from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

import duckdb

PROJECT_ROOT = Path(__file__).resolve().parents[2]
SILVER_JOBS = PROJECT_ROOT / "data" / "silver" / "jobs"
PROCESSING_DIR = PROJECT_ROOT / "src" / "processing"
if str(PROCESSING_DIR) not in sys.path:
    sys.path.insert(0, str(PROCESSING_DIR))

from pipeline_policy import contract_stamp  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.processing.normalize_silver_jobs")

VARCHAR_COLUMNS = (
    "pipeline_run_id",
    "focus_area",
    "mobility_signals",
    "filter_version",
    "job_type",
    "salary_raw",
    "source",
    "source_job_id",
    "source_site",
    "data_contract_version",
    "silver_jobs_schema_version",
    "geo_prompt_version",
)


def _job_files(root: Path = SILVER_JOBS) -> list[Path]:
    return sorted(root.glob("year=*/month=*/day=*/jobs_clean.parquet"))


def _physical_columns(con: duckdb.DuckDBPyConnection, path: Path) -> list[str]:
    rows = con.execute(
        "describe select * from read_parquet(?)",
        [str(path)],
    ).fetchall()
    return [row[0] for row in rows]


def _normalize_sql(path: Path, columns: list[str]) -> str:
    replacements = []
    for name in VARCHAR_COLUMNS:
        if name in columns:
            replacements.append(f'cast("{name}" as varchar) as "{name}"')
    select_body = "* replace (" + ", ".join(replacements) + ")" if replacements else "*"
    path_sql = path.as_posix().replace("'", "''")
    stub = contract_stamp()
    stub_select = ",\n            ".join(
        f"null::varchar as {key}" for key in stub
    )
    return f"""
    select * from (
        select {select_body}
        from read_parquet('{path_sql}')
        union all by name
        select
            {stub_select}
        where 1 = 0
    )
    """


def normalize_file(con: duckdb.DuckDBPyConnection, path: Path) -> None:
    columns = _physical_columns(con, path)
    sql = _normalize_sql(path, columns)
    tmp = path.with_name(".jobs_clean.normalized.tmp.parquet")
    out_sql = tmp.as_posix().replace("'", "''")
    con.execute(
        f"copy ({sql}) to '{out_sql}' (format parquet, compression zstd)"
    )
    tmp.replace(path)


def _s3_key(path: Path) -> str:
    year = next(part for part in path.parts if part.startswith("year="))
    month = next(part for part in path.parts if part.startswith("month="))
    day = next(part for part in path.parts if part.startswith("day="))
    return f"silver/jobs/{year}/{month}/{day}/jobs_clean.parquet"


def main() -> int:
    files = _job_files()
    if not files:
        logger.warning("No Silver jobs_clean.parquet files to normalize")
        return 0

    upload = os.getenv("CANADAPT_REWRITE_SILVER_S3", "1").strip().lower() not in {
        "0",
        "false",
        "no",
    }
    from process_silver import load_env, upload_file_to_s3

    load_env()
    rewritten = 0
    with duckdb.connect(":memory:") as con:
        for path in files:
            logger.info("Normalizing Silver jobs partition | %s", path)
            normalize_file(con, path)
            rewritten += 1
            if upload:
                try:
                    upload_file_to_s3(path, _s3_key(path))
                except Exception as exc:  # noqa: BLE001
                    logger.warning("Could not upload normalized Silver | %s | %s", path, exc)

    logger.info("Normalized %d Silver job partition(s)", rewritten)
    return 0


if __name__ == "__main__":
    sys.exit(main())
