"""CanAdapt — Jooble Bronze ingestion for mobility-focused Canadian jobs.

The source is intentionally narrow: only technology or banking operations
roles that also expose an immigration/mobility signal are retained. The
default query set costs eight Jooble requests per weekly run.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mobility_filter import (
    JOOBLE_FILTER_VERSION,
    has_negative_mobility,
    mobility_signals,
    plain_text,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "jooble"
JOOBLE_URL = "https://jooble.org/api/{api_key}"

RESULTS_PER_PAGE = 50
REQUEST_TIMEOUT_SECONDS = 30
PAUSE_BETWEEN_REQUESTS_SECONDS = 1.0
MAX_RETRIES = 4
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)

# Eight calls/week keep a 500-request key usable for roughly 62 weekly runs.
QUERY_SPECS: tuple[dict[str, str], ...] = (
    {"label": "tech_software", "area": "technology", "keywords": "software developer visa sponsorship"},
    {"label": "tech_data", "area": "technology", "keywords": "data engineer visa sponsorship"},
    {"label": "tech_cloud_security", "area": "technology", "keywords": "cloud cybersecurity LMIA"},
    {"label": "tech_relocation", "area": "technology", "keywords": "technology relocation Canada"},
    {"label": "bank_operations", "area": "banking_operations", "keywords": "bank operations visa sponsorship"},
    {"label": "bank_backoffice", "area": "banking_operations", "keywords": "back office banking relocation"},
    {"label": "bank_middleoffice", "area": "banking_operations", "keywords": "middle office risk compliance sponsorship"},
    {"label": "bank_fincrime", "area": "banking_operations", "keywords": "AML KYC treasury work permit"},
)

TECH_PATTERN = re.compile(
    r"\b(?:software|developer|programmer|data|analytics?|business intelligence|"
    r"cyber(?:security)?|information security|cloud|devops|sre|systems? engineer|"
    r"network engineer|database|machine learning|artificial intelligence|"
    r"full[\s-]?stack|front[\s-]?end|back[\s-]?end|qa engineer|technology|"
    r"technologie|développeur|données|informatique)\b",
    re.IGNORECASE,
)
BANKING_PATTERN = re.compile(
    r"\b(?:bank(?:ing)?|financial institution|capital markets?|investment|"
    r"back[\s-]?office|middle[\s-]?office|trade support|settlements?|"
    r"reconciliation|treasury|cash management|risk|compliance|regulatory|"
    r"anti[\s-]?money laundering|aml|know your customer|kyc|financial crime|"
    r"fraud|credit operations?|loan operations?|payment operations?|"
    r"securities operations?|fund operations?|wealth operations?|"
    r"opérations bancaires|conformité|trésorerie)\b",
    re.IGNORECASE,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.ingestion.jooble")


def load_api_key() -> str:
    load_dotenv(PROJECT_ROOT / ".env")
    api_key = os.getenv("JOOBLE_API_KEY", "").strip()
    if not api_key:
        raise EnvironmentError(
            "Missing JOOBLE_API_KEY. Set it in the project .env or CI secrets."
        )
    if not re.fullmatch(r"[0-9a-fA-F-]{36}", api_key):
        raise EnvironmentError("JOOBLE_API_KEY does not have the expected GUID format.")
    return api_key


def build_session() -> requests.Session:
    retry = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        status=MAX_RETRIES,
        backoff_factor=1.5,
        status_forcelist=RETRYABLE_STATUS_CODES,
        allowed_methods=frozenset({"POST"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    session = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    return session


def classify_area(text: str) -> str | None:
    """Return the allowed CanAdapt area for a listing, or None."""
    if TECH_PATTERN.search(text):
        return "technology"
    if BANKING_PATTERN.search(text):
        return "banking_operations"
    return None


def filter_job(job: dict[str, Any], expected_area: str) -> dict[str, Any] | None:
    """Enforce both the professional-area and mobility contracts."""
    text = plain_text(
        " ".join(str(job.get(field) or "") for field in ("title", "snippet", "company", "type"))
    )
    area = classify_area(text)
    signals = mobility_signals(text)
    if area != expected_area or not signals or has_negative_mobility(text):
        return None
    if not job.get("id") or not job.get("title") or not job.get("link"):
        return None
    return {
        **job,
        "canadapt_area": area,
        "canadapt_mobility_signals": signals,
        "canadapt_filter_version": JOOBLE_FILTER_VERSION,
    }


def request_query(
    session: requests.Session,
    api_key: str,
    spec: dict[str, str],
) -> dict[str, Any]:
    response = session.post(
        JOOBLE_URL.format(api_key=api_key),
        json={
            "keywords": spec["keywords"],
            "location": "Canada",
            "radius": "80",
            "page": "1",
            "ResultOnPage": str(RESULTS_PER_PAGE),
            "SearchMode": "1",
            "companysearch": "false",
        },
        timeout=REQUEST_TIMEOUT_SECONDS,
    )
    if response.status_code == 403:
        raise RuntimeError("Jooble rejected the API key or its request quota is exhausted.")
    response.raise_for_status()
    try:
        payload = response.json()
    except ValueError as exc:
        raise RuntimeError("Jooble returned a non-JSON response.") from exc
    jobs = payload.get("jobs")
    if not isinstance(jobs, list):
        raise RuntimeError("Jooble response is missing the jobs list.")
    return payload


def fetch_jooble_jobs(
    session: requests.Session,
    api_key: str,
) -> dict[str, Any]:
    unique: dict[str, dict[str, Any]] = {}
    query_metrics: list[dict[str, Any]] = []
    errors: list[dict[str, str]] = []

    for index, spec in enumerate(QUERY_SPECS):
        if index:
            time.sleep(PAUSE_BETWEEN_REQUESTS_SECONDS)
        try:
            payload = request_query(session, api_key, spec)
        except (requests.RequestException, RuntimeError) as exc:
            logger.error("Jooble query %s failed: %s", spec["label"], exc)
            errors.append({"query": spec["label"], "error": str(exc)})
            continue

        raw_jobs = payload.get("jobs") or []
        kept = 0
        for raw_job in raw_jobs:
            job = filter_job(raw_job, spec["area"])
            if job is None:
                continue
            source_id = str(job["id"])
            if source_id in unique:
                labels = unique[source_id].setdefault("canadapt_queries", [])
                if spec["label"] not in labels:
                    labels.append(spec["label"])
                continue
            job["canadapt_queries"] = [spec["label"]]
            unique[source_id] = job
            kept += 1

        query_metrics.append(
            {
                "label": spec["label"],
                "area": spec["area"],
                "keywords": spec["keywords"],
                "api_count": int(payload.get("totalCount") or 0),
                "results_fetched": len(raw_jobs),
                "results_kept": kept,
            }
        )
        logger.info(
            "Jooble query=%s | fetched=%d | kept=%d",
            spec["label"],
            len(raw_jobs),
            kept,
        )

    if not query_metrics:
        raise RuntimeError(
            "Every Jooble query failed. "
            f"First error: {errors[0]['error'] if errors else 'unknown'}"
        )
    return {
        "results": list(unique.values()),
        "queries": query_metrics,
        "query_errors": errors,
        "requests_used": len(query_metrics) + len(errors),
        "results_unique": len(unique),
    }


def partition_parts(now: datetime) -> tuple[str, str, str]:
    return (
        f"year={now.year}",
        f"month={now.month:02d}",
        f"day={now.day:02d}",
    )


def save_bronze(document: dict[str, Any], now: datetime) -> tuple[Path, str]:
    parts = partition_parts(now)
    out_dir = BRONZE_ROOT.joinpath(*parts)
    out_dir.mkdir(parents=True, exist_ok=True)
    filename = f"jooble_raw_{now.strftime('%H%M%S')}.json"
    path = out_dir / filename
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    return path, f"bronze/jooble/{'/'.join(parts)}/{filename}"


def upload_bronze(document: dict[str, Any], s3_key: str) -> str:
    load_dotenv(PROJECT_ROOT / ".env")
    bucket = (
        os.getenv("AWS_BUCKET_NAME", "").strip()
        or os.getenv("AWS_S3_BUCKET_NAME", "").strip()
    )
    if not bucket:
        raise EnvironmentError("Missing AWS_BUCKET_NAME or AWS_S3_BUCKET_NAME.")
    region = os.getenv("AWS_DEFAULT_REGION", "").strip() or "us-east-1"
    try:
        boto3.client("s3", region_name=region).put_object(
            Bucket=bucket,
            Key=s3_key,
            Body=json.dumps(document, ensure_ascii=False).encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(f"S3 Jooble upload failed ({code}): {exc}") from exc
    return f"s3://{bucket}/{s3_key}"


def main() -> int:
    try:
        api_key = load_api_key()
        now_local = datetime.now().astimezone()
        extracted_at = datetime.now(timezone.utc)
        with build_session() as session:
            payload = fetch_jooble_jobs(session, api_key)
        document = {
            "source": "jooble",
            "country": "ca",
            "extracted_at_utc": extracted_at.isoformat(),
            "filter_contract": {
                "allowed_areas": ["technology", "banking_operations"],
                "mobility_required": True,
                "version": JOOBLE_FILTER_VERSION,
            },
            "results_per_page": RESULTS_PER_PAGE,
            "requests_used": payload["requests_used"],
            "queries": payload["queries"],
            "errors": payload["query_errors"],
            "payload": payload,
        }
        local_path, s3_key = save_bronze(document, now_local)
        s3_uri = upload_bronze(document, s3_key)
        logger.info(
            "Jooble Bronze complete | requests=%d | jobs=%d | local=%s | s3=%s",
            payload["requests_used"],
            payload["results_unique"],
            local_path,
            s3_uri,
        )
        return 0
    except (
        EnvironmentError,
        OSError,
        RuntimeError,
        requests.RequestException,
    ) as exc:
        logger.error("Jooble ingestion failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
