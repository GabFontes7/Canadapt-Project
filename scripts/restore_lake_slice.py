"""Restore durable caches/wages and hive snapshots within retention."""

from __future__ import annotations

import logging
import os
import sys
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSING_DIR = PROJECT_ROOT / "src" / "processing"
if str(PROCESSING_DIR) not in sys.path:
    sys.path.insert(0, str(PROCESSING_DIR))

from pipeline_policy import durable_prefixes, hive_date_from_key, lake_retention_days  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.restore_lake_slice")

SNAPSHOT_PREFIXES = (
    "bronze/year=",
    "bronze/jooble/",
    "bronze/cost_of_living/",
    "silver/jobs/",
    "silver/cost_of_living/",
)


def _bucket() -> str:
    bucket = (
        os.getenv("AWS_BUCKET_NAME", "").strip()
        or os.getenv("AWS_S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        raise EnvironmentError("Missing AWS_BUCKET_NAME / AWS_S3_BUCKET_NAME")
    return bucket


def _s3_client():
    import boto3

    kwargs: dict[str, str] = {}
    key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "").strip() or "us-east-1"
    if key and secret:
        kwargs["aws_access_key_id"] = key
        kwargs["aws_secret_access_key"] = secret
    kwargs["region_name"] = region
    return boto3.client("s3", **kwargs)


def download_key(client, bucket: str, key: str) -> None:
    dest = PROJECT_ROOT / "data" / key
    dest.parent.mkdir(parents=True, exist_ok=True)
    client.download_file(bucket, key, str(dest))


def iter_keys(client, bucket: str, prefix: str):
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents") or []:
            key = obj["Key"]
            if not key.endswith("/"):
                yield key


def sync_prefix(client, bucket: str, prefix: str, cutoff: date | None = None) -> int:
    copied = 0
    for key in iter_keys(client, bucket, prefix):
        if cutoff is not None:
            day = hive_date_from_key(key)
            if day is None or day < cutoff:
                continue
        download_key(client, bucket, key)
        copied += 1
    return copied


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    days = lake_retention_days()
    cutoff = datetime.now(timezone.utc).date() - timedelta(days=days - 1)
    bucket = _bucket()
    client = _s3_client()
    logger.info(
        "Restoring lake slice | bucket=%s | retention_days=%d | cutoff=%s",
        bucket,
        days,
        cutoff.isoformat(),
    )
    total = 0
    for prefix in durable_prefixes():
        copied = sync_prefix(client, bucket, prefix)
        logger.info("Durable prefix | objects=%d | %s", copied, prefix)
        total += copied
    for prefix in SNAPSHOT_PREFIXES:
        copied = sync_prefix(client, bucket, prefix, cutoff=cutoff)
        logger.info("Snapshot prefix | objects=%d | %s", copied, prefix)
        total += copied
    logger.info("Lake slice restore complete | objects=%d", total)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:  # noqa: BLE001
        logger.error("Lake slice restore failed: %s", exc)
        raise SystemExit(1)
