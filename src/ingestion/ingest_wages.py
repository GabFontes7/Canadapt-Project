"""Ingest the latest official Canadian wage benchmark (ESDC) to Bronze."""

from __future__ import annotations

import json
import logging
import os
import re
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import requests
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATASET_ID = "adad580f-76b0-4502-bd05-20c125de9116"
METADATA_URL = (
    "https://open.canada.ca/data/api/3/action/package_show"
    f"?id={DATASET_ID}"
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.ingestion.wages")


def _session() -> requests.Session:
    retry = Retry(
        total=4,
        connect=4,
        read=4,
        status=4,
        backoff_factor=1.5,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=frozenset({"GET"}),
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def _latest_csv_resource(session: requests.Session) -> tuple[int, dict[str, Any]]:
    response = session.get(METADATA_URL, timeout=30)
    response.raise_for_status()
    payload = response.json()
    if not payload.get("success"):
        raise RuntimeError("Open Government metadata API returned success=false")

    candidates: list[tuple[int, dict[str, Any]]] = []
    for resource in payload["result"].get("resources", []):
        name = str(resource.get("name") or "")
        match = re.search(r"\b(20\d{2})\b", name)
        if match and str(resource.get("format") or "").upper() == "CSV":
            candidates.append((int(match.group(1)), resource))
    if not candidates:
        raise RuntimeError("No annual CSV resource found in official wage dataset")
    return max(candidates, key=lambda item: item[0])


def _bucket_and_client():
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
    load_dotenv(PROJECT_ROOT / ".env")
    try:
        with _session() as session:
            reference_year, resource = _latest_csv_resource(session)
            out_dir = (
                PROJECT_ROOT
                / "data"
                / "bronze"
                / "wages"
                / f"reference_year={reference_year}"
            )
            out_dir.mkdir(parents=True, exist_ok=True)
            csv_path = out_dir / "wages_official.csv"
            metadata_path = out_dir / "metadata.json"

            if not csv_path.exists():
                logger.info(
                    "Downloading official ESDC wages | year=%d | url=%s",
                    reference_year,
                    resource["url"],
                )
                response = session.get(resource["url"], timeout=120)
                response.raise_for_status()
                tmp_path = csv_path.with_suffix(".tmp")
                tmp_path.write_bytes(response.content)
                tmp_path.replace(csv_path)
            else:
                logger.info("Official wage CSV already cached locally: %s", csv_path)

            metadata = {
                "dataset_id": DATASET_ID,
                "dataset_url": f"https://open.canada.ca/data/en/dataset/{DATASET_ID}",
                "resource_name": resource.get("name"),
                "resource_url": resource.get("url"),
                "reference_year": reference_year,
                "retrieved_at_utc": datetime.now(timezone.utc).isoformat(),
                "publisher": "Employment and Social Development Canada",
                "licence": "Open Government Licence - Canada",
            }
            metadata_path.write_text(
                json.dumps(metadata, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        bucket, s3 = _bucket_and_client()
        prefix = f"bronze/wages/reference_year={reference_year}"
        s3.upload_file(str(csv_path), bucket, f"{prefix}/wages_official.csv")
        s3.upload_file(
            str(metadata_path),
            bucket,
            f"{prefix}/metadata.json",
            ExtraArgs={"ContentType": "application/json"},
        )
        logger.info(
            "Official wages Dual-Write complete | local=%s | s3=s3://%s/%s/",
            csv_path,
            bucket,
            prefix,
        )
        return 0
    except (requests.RequestException, OSError, RuntimeError, EnvironmentError) as exc:
        logger.error("Official wage ingestion failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
