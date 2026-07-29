"""
CanAdapt — Bronze ingestion (local + AWS S3 Dual-Write).

Pulls Adzuna Canada ads focused on visa sponsorship / LMIA / relocation,
retries transient API failures, then writes the same Hive-partitioned JSON
locally and to S3 (Dual-Write). Profession categorization happens in later layers.
"""

from __future__ import annotations

import json
import logging
import math
import os
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

# -----------------------------------------------------------------------------
# Config
# -----------------------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze"

ADZUNA_SEARCH_URL = "https://api.adzuna.com/v1/api/jobs/ca/search/{page}"

# Native Adzuna params: sponsorship focus + broader visa signals via what_or.
SEARCH_WHAT = "visa sponsorship"
SEARCH_WHAT_OR = "LMIA sponsorship relocation"

RESULTS_PER_PAGE = 50
MAX_DAYS_OLD = 7  # weekly cadence
REQUEST_TIMEOUT_SECONDS = 30
PAUSE_BETWEEN_REQUESTS_SECONDS = 2.5

# urllib3 / requests retry for 429 and 5xx (up to 4 retries).
MAX_RETRIES = 4
RETRY_BACKOFF_FACTOR = 1.5
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.ingestion.adzuna")


def load_credentials() -> tuple[str, str]:
    """Load Adzuna credentials from .env (project root)."""
    load_dotenv(PROJECT_ROOT / ".env")

    app_id = os.getenv("ADZUNA_APP_ID", "").strip()
    app_key = os.getenv("ADZUNA_APP_KEY", "").strip()

    if not app_id or not app_key:
        raise EnvironmentError(
            "Missing ADZUNA_APP_ID or ADZUNA_APP_KEY. "
            "Set them in the .env file at the project root."
        )

    placeholders = {"seu_app_id_real", "sua_app_key_real"}
    if app_id in placeholders or app_key in placeholders:
        raise EnvironmentError(
            "Replace the placeholder values in .env with your real "
            "Adzuna APP_ID and APP_KEY before running ingestion."
        )

    return app_id, app_key


def build_session() -> requests.Session:
    """HTTP session with urllib3 Retry for transient Adzuna failures."""
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        status=MAX_RETRIES,
        backoff_factor=RETRY_BACKOFF_FACTOR,
        status_forcelist=RETRYABLE_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def _request_page(
    session: requests.Session,
    app_id: str,
    app_key: str,
    page: int,
) -> dict[str, Any]:
    """GET one Adzuna page using native visa-focused params."""
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": SEARCH_WHAT,
        "what_or": SEARCH_WHAT_OR,
        "results_per_page": RESULTS_PER_PAGE,
        "max_days_old": MAX_DAYS_OLD,
        "sort_by": "date",
        "content-type": "application/json",
    }
    url = ADZUNA_SEARCH_URL.format(page=page)
    last_error: Exception | None = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                url,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code in RETRYABLE_STATUS_CODES:
                delay = RETRY_BACKOFF_FACTOR * (2 ** (attempt - 1))
                logger.warning(
                    "Adzuna HTTP %s for page=%d (attempt %d/%d). "
                    "Manual backoff %.1fs…",
                    response.status_code,
                    page,
                    attempt,
                    MAX_RETRIES,
                    delay,
                )
                time.sleep(delay)
                last_error = RuntimeError(
                    f"Adzuna HTTP {response.status_code} for page={page}. "
                    f"Body: {(response.text or '')[:300]}"
                )
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.Timeout as exc:
            delay = RETRY_BACKOFF_FACTOR * (2 ** (attempt - 1))
            logger.warning(
                "Timeout page=%d (attempt %d/%d). Backoff %.1fs…",
                page,
                attempt,
                MAX_RETRIES,
                delay,
            )
            time.sleep(delay)
            last_error = ConnectionError(
                f"Timeout talking to Adzuna for page={page} "
                f"(>{REQUEST_TIMEOUT_SECONDS}s)."
            )
            last_error.__cause__ = exc
        except requests.exceptions.ConnectionError as exc:
            delay = RETRY_BACKOFF_FACTOR * (2 ** (attempt - 1))
            logger.warning(
                "Network error page=%d (attempt %d/%d). Backoff %.1fs…",
                page,
                attempt,
                MAX_RETRIES,
                delay,
            )
            time.sleep(delay)
            last_error = ConnectionError(
                f"Network error reaching Adzuna for page={page}."
            )
            last_error.__cause__ = exc
        except requests.exceptions.HTTPError as exc:
            status = getattr(exc.response, "status_code", "?")
            body = ""
            if exc.response is not None:
                body = (exc.response.text or "")[:300]
            raise RuntimeError(
                f"Adzuna HTTP {status} for page={page}. Body: {body}"
            ) from exc
        except ValueError as exc:
            raise RuntimeError(
                f"Adzuna returned non-JSON for page={page}."
            ) from exc

    assert last_error is not None
    raise last_error


def fetch_adzuna_jobs(
    session: requests.Session,
    app_id: str,
    app_key: str,
) -> dict[str, Any]:
    """Fetch all pages for the sponsorship-focused query (last MAX_DAYS_OLD)."""
    logger.info(
        "Fetching Adzuna CA | what=%r | what_or=%r | max_days_old=%d",
        SEARCH_WHAT,
        SEARCH_WHAT_OR,
        MAX_DAYS_OLD,
    )

    first = _request_page(session, app_id, app_key, page=1)
    total_count = int(first.get("count") or 0)
    pages = max(1, math.ceil(total_count / RESULTS_PER_PAGE)) if total_count else 1

    all_results: list[dict[str, Any]] = list(first.get("results") or [])
    page_payloads: list[dict[str, Any]] = [first]

    logger.info(
        "Page 1/%d | api_count=%d | page_results=%d",
        pages,
        total_count,
        len(first.get("results") or []),
    )

    for page in range(2, pages + 1):
        time.sleep(PAUSE_BETWEEN_REQUESTS_SECONDS)
        payload = _request_page(session, app_id, app_key, page=page)
        page_results = list(payload.get("results") or [])
        all_results.extend(page_results)
        page_payloads.append(payload)
        logger.info(
            "Page %d/%d | page_results=%d | accumulated=%d",
            page,
            pages,
            len(page_results),
            len(all_results),
        )

    unique: dict[str, dict[str, Any]] = {}
    for job in all_results:
        job_id = str(job.get("id") or "")
        if job_id and job_id not in unique:
            unique[job_id] = job
    deduped = list(unique.values()) if unique else all_results

    logger.info(
        "Fetch complete | api_count=%d | fetched=%d | unique=%d | pages=%d",
        total_count,
        len(all_results),
        len(deduped),
        pages,
    )

    return {
        "what": SEARCH_WHAT,
        "what_or": SEARCH_WHAT_OR,
        "max_days_old": MAX_DAYS_OLD,
        "api_count": total_count,
        "pages_fetched": pages,
        "results_fetched": len(all_results),
        "results_unique": len(deduped),
        "mean": first.get("mean"),
        "results": deduped,
        "raw_pages": page_payloads,
    }


def bronze_partition_parts(now: datetime) -> tuple[str, str, str]:
    """Return year=/month=/day= partition segments for local + S3 paths."""
    return (
        f"year={now.year}",
        f"month={now.month:02d}",
        f"day={now.day:02d}",
    )


def bronze_s3_key(now: datetime, filename: str) -> str:
    """S3 object key mirroring local Hive partitions under bronze/."""
    year_p, month_p, day_p = bronze_partition_parts(now)
    return f"bronze/{year_p}/{month_p}/{day_p}/{filename}"


def save_bronze_payload(payload: dict[str, Any], now: datetime) -> tuple[Path, str]:
    """Write bronze JSON locally; return (local_path, s3_key)."""
    year_p, month_p, day_p = bronze_partition_parts(now)
    out_dir = BRONZE_ROOT / year_p / month_p / day_p
    out_dir.mkdir(parents=True, exist_ok=True)

    filename = f"adzuna_raw_{now.strftime('%H%M%S')}.json"
    out_path = out_dir / filename
    s3_key = bronze_s3_key(now, filename)

    with out_path.open("w", encoding="utf-8") as fh:
        json.dump(payload, fh, ensure_ascii=False, indent=2)

    logger.info("Bronze local file written: %s", out_path)
    return out_path, s3_key


def salvar_camada_bronze_s3(dados: dict[str, Any], caminho_s3: str) -> str:
    """
    Dual-Write: upload the same bronze JSON payload to AWS S3.

    Credentials and bucket are read explicitly from the project `.env`:
    AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION, AWS_S3_BUCKET_NAME.
    """
    load_dotenv(PROJECT_ROOT / ".env")

    access_key = os.getenv("AWS_ACCESS_KEY_ID", "").strip()
    secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "").strip()
    region = os.getenv("AWS_DEFAULT_REGION", "").strip() or "us-east-1"
    bucket = os.getenv("AWS_S3_BUCKET_NAME", "").strip()

    if not access_key or not secret_key or not bucket:
        raise EnvironmentError(
            "Missing AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, or "
            "AWS_S3_BUCKET_NAME in the .env file."
        )

    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
    )

    body = json.dumps(dados, ensure_ascii=False)

    try:
        s3.put_object(
            Bucket=bucket,
            Key=caminho_s3,
            Body=body.encode("utf-8"),
            ContentType="application/json; charset=utf-8",
        )
    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(
            f"S3 put_object failed ({error_code}) for s3://{bucket}/{caminho_s3}: {exc}"
        ) from exc

    uri = f"s3://{bucket}/{caminho_s3}"
    logger.info("Bronze S3 object written: %s", uri)
    return uri


def build_bronze_document(
    session: requests.Session,
    app_id: str,
    app_key: str,
    extracted_at: datetime,
) -> dict[str, Any]:
    """Assemble dual-write-ready bronze document with all visa-related ads."""
    errors: list[dict[str, str]] = []
    block: dict[str, Any] | None = None

    try:
        block = fetch_adzuna_jobs(session, app_id, app_key)
    except (ConnectionError, RuntimeError) as exc:
        logger.error("%s", exc)
        errors.append({"query": SEARCH_WHAT, "error": str(exc)})

    if block is None:
        raise RuntimeError(
            "Adzuna query failed; nothing to write to Bronze. "
            f"Error: {errors[0]['error'] if errors else 'unknown'}"
        )

    return {
        "source": "adzuna",
        "country": "ca",
        "endpoint_template": ADZUNA_SEARCH_URL,
        "extracted_at_utc": extracted_at.isoformat(),
        "max_days_old": MAX_DAYS_OLD,
        "what": SEARCH_WHAT,
        "what_or": SEARCH_WHAT_OR,
        "results_per_page": RESULTS_PER_PAGE,
        "total_results_fetched": block.get("results_fetched") or 0,
        "total_results_unique": block.get("results_unique") or 0,
        "payload": block,
        "errors": errors,
    }


def main() -> int:
    try:
        app_id, app_key = load_credentials()
    except EnvironmentError as exc:
        logger.error("%s", exc)
        return 1

    now_local = datetime.now().astimezone()
    extracted_at = datetime.now(timezone.utc)
    session = build_session()

    try:
        document = build_bronze_document(session, app_id, app_key, extracted_at)
        local_path, s3_key = save_bronze_payload(document, now_local)
        s3_uri = salvar_camada_bronze_s3(document, s3_key)
    except (ConnectionError, RuntimeError, OSError, EnvironmentError) as exc:
        logger.error("Ingestion failed: %s", exc)
        return 1
    finally:
        session.close()

    logger.info(
        "Dual-Write finished | max_days_old=%d | fetched=%d | "
        "unique=%d | local=%s | s3=%s",
        MAX_DAYS_OLD,
        document.get("total_results_fetched") or 0,
        document.get("total_results_unique") or 0,
        local_path,
        s3_uri,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
