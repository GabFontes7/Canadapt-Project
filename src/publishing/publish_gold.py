"""Publish canonical Gold tables as partitioned Parquet locally and on S3."""

from __future__ import annotations

import logging
import os
import shutil
import sys
from datetime import date
from pathlib import Path

import boto3
import duckdb
from botocore.exceptions import BotoCoreError, ClientError
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DUCKDB_PATH = Path(
    os.getenv(
        "CANADAPT_DUCKDB_PATH",
        str(PROJECT_ROOT / "data" / "gold" / "canadapt_analytics.duckdb"),
    )
)
GOLD_PARQUET_ROOT = PROJECT_ROOT / "data" / "gold" / "parquet"

GOLD_TABLES = (
    "dim_geografia_custos",
    "dim_vaga",
    "fct_vagas_snapshot",
    "fato_vagas_visto",
    "fct_viabilidade_vagas",
    "cenarios_vaga_remota",
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.publishing.gold")


def _partition_for_latest_snapshot(con: duckdb.DuckDBPyConnection) -> tuple[str, str, str]:
    snapshot_date = con.execute(
        "select max(data_snapshot) from main.fct_vagas_snapshot"
    ).fetchone()[0]
    if snapshot_date is None:
        snapshot_date = date.today()
    return (
        f"year={snapshot_date.year}",
        f"month={snapshot_date.month:02d}",
        f"day={snapshot_date.day:02d}",
    )


def _export_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    partition: tuple[str, str, str],
) -> tuple[Path, int]:
    partition_dir = GOLD_PARQUET_ROOT / table / Path(*partition)
    partition_dir.mkdir(parents=True, exist_ok=True)
    out_path = partition_dir / f"{table}.parquet"
    tmp_path = partition_dir / f".{table}.tmp.parquet"

    escaped_tmp = tmp_path.as_posix().replace("'", "''")
    con.execute(
        f"COPY (SELECT * FROM main.{table}) TO '{escaped_tmp}' "
        "(FORMAT PARQUET, COMPRESSION ZSTD)"
    )
    tmp_path.replace(out_path)

    latest_dir = GOLD_PARQUET_ROOT / table / "latest"
    latest_dir.mkdir(parents=True, exist_ok=True)
    shutil.copy2(out_path, latest_dir / f"{table}.parquet")

    rows = con.execute(f"select count(*) from main.{table}").fetchone()[0]
    return out_path, int(rows)


def _s3_client_and_bucket():
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = (
        os.getenv("AWS_BUCKET_NAME", "").strip()
        or os.getenv("AWS_S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        raise EnvironmentError(
            "Missing AWS_BUCKET_NAME (or AWS_S3_BUCKET_NAME) for Gold publishing."
        )
    region = os.getenv("AWS_DEFAULT_REGION", "").strip() or "us-east-1"
    return boto3.client("s3", region_name=region), bucket


def main() -> int:
    if not DUCKDB_PATH.exists():
        logger.error("Gold DuckDB not found: %s", DUCKDB_PATH)
        return 1

    try:
        s3, bucket = _s3_client_and_bucket()
        with duckdb.connect(str(DUCKDB_PATH), read_only=True) as con:
            partition = _partition_for_latest_snapshot(con)
            partition_key = "/".join(partition)

            total_rows = 0
            for table in GOLD_TABLES:
                local_path, rows = _export_table(con, table, partition)
                s3_key = f"gold/{table}/{partition_key}/{table}.parquet"
                s3.upload_file(str(local_path), bucket, s3_key)
                latest_key = f"gold/{table}/latest/{table}.parquet"
                s3.upload_file(str(local_path), bucket, latest_key)
                logger.info(
                    "Gold published | table=%s | rows=%d | local=%s | "
                    "s3=s3://%s/%s | latest=s3://%s/%s",
                    table,
                    rows,
                    local_path,
                    bucket,
                    s3_key,
                    bucket,
                    latest_key,
                )
                total_rows += rows

        logger.info(
            "Gold Parquet publishing complete | tables=%d | total_rows=%d",
            len(GOLD_TABLES),
            total_rows,
        )
        return 0
    except (duckdb.Error, OSError, EnvironmentError, BotoCoreError, ClientError) as exc:
        logger.error("Gold publishing failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
