"""Normalize the official ESDC wage CSV from Bronze into Silver Parquet."""

from __future__ import annotations

import json
import logging
import os
import sys
from pathlib import Path

import boto3
import duckdb
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_WAGES_ROOT = PROJECT_ROOT / "data" / "bronze" / "wages"
SILVER_WAGES_ROOT = PROJECT_ROOT / "data" / "silver" / "wages"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.processing.wages")


def _latest_source() -> tuple[int, Path, Path]:
    metadata_files = list(BRONZE_WAGES_ROOT.glob("reference_year=*/metadata.json"))
    if not metadata_files:
        raise FileNotFoundError(f"No official wage metadata under {BRONZE_WAGES_ROOT}")
    candidates = []
    for metadata_path in metadata_files:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
        year = int(metadata["reference_year"])
        csv_path = metadata_path.parent / "wages_official.csv"
        if csv_path.exists():
            candidates.append((year, csv_path, metadata_path))
    if not candidates:
        raise FileNotFoundError("No complete official wage Bronze partition found")
    return max(candidates, key=lambda item: item[0])


def _s3_target():
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = (
        os.getenv("AWS_BUCKET_NAME", "").strip()
        or os.getenv("AWS_S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        raise EnvironmentError("Missing AWS_BUCKET_NAME or AWS_S3_BUCKET_NAME")
    region = os.getenv("AWS_DEFAULT_REGION", "").strip() or "us-east-1"
    return bucket, boto3.client("s3", region_name=region)


def main() -> int:
    try:
        reference_year, csv_path, _ = _latest_source()
        out_dir = SILVER_WAGES_ROOT / f"reference_year={reference_year}"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "wages_official.parquet"
        tmp_path = out_dir / ".wages_official.tmp.parquet"
        csv_sql = csv_path.as_posix().replace("'", "''")
        tmp_sql = tmp_path.as_posix().replace("'", "''")

        with duckdb.connect(":memory:") as con:
            con.execute(
                f"""
                COPY (
                    WITH source AS (
                        SELECT *
                        FROM read_csv_auto(
                            '{csv_sql}',
                            header = true,
                            sample_size = -1,
                            ignore_errors = true
                        )
                    )
                    SELECT
                        regexp_replace(cast(NOC_CNP as varchar), '^NOC_', '') as noc_code,
                        cast(NOC_Title_eng as varchar) as noc_title,
                        cast(prov as varchar) as province_code,
                        cast(ER_Code_Code_RE as varchar) as economic_region_code,
                        cast(ER_Name as varchar) as economic_region_name,
                        try_cast(Low_Wage_Salaire_Minium as double) as wage_low_original,
                        try_cast(Median_Wage_Salaire_Median as double) as wage_median_original,
                        try_cast(High_Wage_Salaire_Maximal as double) as wage_high_original,
                        try_cast(Annual_Wage_Flag_Salaire_annuel as integer) = 1
                            as wage_is_annual,
                        case
                            when try_cast(Annual_Wage_Flag_Salaire_annuel as integer) = 1
                                then try_cast(Low_Wage_Salaire_Minium as double)
                            else try_cast(Low_Wage_Salaire_Minium as double) * 2080
                        end as salary_annual_low,
                        case
                            when try_cast(Annual_Wage_Flag_Salaire_annuel as integer) = 1
                                then try_cast(Median_Wage_Salaire_Median as double)
                            else try_cast(Median_Wage_Salaire_Median as double) * 2080
                        end as salary_annual_median,
                        case
                            when try_cast(Annual_Wage_Flag_Salaire_annuel as integer) = 1
                                then try_cast(High_Wage_Salaire_Maximal as double)
                            else try_cast(High_Wage_Salaire_Maximal as double) * 2080
                        end as salary_annual_high,
                        try_cast(Reference_Period as integer) as source_reference_period,
                        try_cast(Revision_Date_Date_revision as date) as source_revision_date,
                        {reference_year}::integer as dataset_reference_year,
                        'ESDC Open Government Wages' as salary_source
                    FROM source
                    WHERE NOC_CNP IS NOT NULL
                ) TO '{tmp_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
                """
            )
            rows = con.execute(
                "select count(*) from read_parquet(?)", [str(tmp_path)]
            ).fetchone()[0]
        tmp_path.replace(out_path)

        bucket, s3 = _s3_target()
        key = (
            f"silver/wages/reference_year={reference_year}/"
            "wages_official.parquet"
        )
        s3.upload_file(str(out_path), bucket, key)
        logger.info(
            "Official wages Silver complete | rows=%d | local=%s | s3=s3://%s/%s",
            rows,
            out_path,
            bucket,
            key,
        )
        return 0
    except (duckdb.Error, OSError, ValueError, EnvironmentError) as exc:
        logger.error("Official wage processing failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
