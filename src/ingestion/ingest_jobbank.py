"""CanAdapt — Job Bank Bronze ingestion (public search, no account).

Collects Canadian Job Bank listings that the government itself tags as:
  - LMIA requested  (fskl=101010)
  - Approved LMIA   (fskl=101020)
  - Open to international candidates (fglo=1)

No API key or Job Bank account is required. Dual-writes Hive-partitioned JSON
locally and to S3.
"""

from __future__ import annotations

import json
import logging
import os
import re
import sys
import time
from datetime import datetime, timezone
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urljoin

import boto3
import requests
from botocore.exceptions import ClientError
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from mobility_filter import plain_text

PROJECT_ROOT = Path(__file__).resolve().parents[2]
BRONZE_ROOT = PROJECT_ROOT / "data" / "bronze" / "jobbank"

JOBBANK_ORIGIN = "https://www.jobbank.gc.ca"
SEARCH_URL = f"{JOBBANK_ORIGIN}/jobsearch/"
DETAIL_URL = f"{JOBBANK_ORIGIN}/jobsearch/jobposting/{{posting_id}}"

JOBBANK_FILTER_VERSION = "jobbank_lmia_intl_v1"

# Last 30 days keeps weekly cadence meaningful without dumping the full archive.
MAX_AGE_DAYS_PARAM = "7"
REQUEST_TIMEOUT_SECONDS = 45
PAUSE_BETWEEN_REQUESTS_SECONDS = 1.0
MAX_RETRIES = 4
RETRYABLE_STATUS_CODES = (429, 500, 502, 503, 504)
MAX_PAGES_PER_QUERY = int(os.getenv("CANADAPT_JOBBANK_MAX_PAGES", "40"))
MAX_JOBS_PER_RUN = int(os.getenv("CANADAPT_MAX_JOBBANK_JOBS", "200"))
FETCH_DETAILS = os.getenv("CANADAPT_JOBBANK_FETCH_DETAILS", "1").strip() not in {
    "0",
    "false",
    "no",
}

# Ordered by priority: structured LMIA first, then broader international openings.
QUERY_SPECS: tuple[dict[str, Any], ...] = (
    {
        "label": "lmia_requested",
        "mobility_signals": ["lmia", "lmia_requested"],
        "params": {"fage": MAX_AGE_DAYS_PARAM, "sort": "D", "fskl": "101010"},
        "priority": 0,
    },
    {
        "label": "lmia_approved",
        "mobility_signals": ["lmia", "lmia_approved"],
        "params": {"fage": MAX_AGE_DAYS_PARAM, "sort": "D", "fskl": "101020"},
        "priority": 0,
    },
    {
        "label": "international_candidates",
        "mobility_signals": ["international_candidates"],
        "params": {"fage": MAX_AGE_DAYS_PARAM, "sort": "D", "fglo": "1"},
        "priority": 1,
    },
)

ARTICLE_RE = re.compile(
    r'<article id="article-(\d+)" class="action-buttons"(.*?)</article>',
    re.IGNORECASE | re.DOTALL,
)
TITLE_RE = re.compile(
    r'<span class="noctitle">\s*(.*?)\s*</span>',
    re.IGNORECASE | re.DOTALL,
)
DATE_RE = re.compile(r'<li class="date">\s*(.*?)\s*</li>', re.IGNORECASE | re.DOTALL)
BUSINESS_RE = re.compile(
    r'<li class="business">\s*(.*?)\s*</li>',
    re.IGNORECASE | re.DOTALL,
)
LOCATION_RE = re.compile(
    r'<li class="location">\s*(.*?)\s*</li>',
    re.IGNORECASE | re.DOTALL,
)
SALARY_RE = re.compile(
    r'<li class="salary">\s*(.*?)\s*</li>',
    re.IGNORECASE | re.DOTALL,
)
JOB_NUMBER_RE = re.compile(
    r'<li class="source">.*?<span class="wb-inv">Job number:</span>\s*'
    r'(?:<span[^>]*>.*?</span>\s*)?(\d+)',
    re.IGNORECASE | re.DOTALL,
)
LMIA_REQUESTED_RE = re.compile(
    r'jobLMIAflag[^"]*submitted|class="[^"]*jobLMIAflag[^"]*"[^>]*>\s*LMIA requested',
    re.IGNORECASE,
)
LMIA_APPROVED_RE = re.compile(
    r'jobLMIAflag[^"]*approved|class="[^"]*jobLMIAflag[^"]*"[^>]*>\s*Approved LMIA',
    re.IGNORECASE,
)
LMIA_REQUESTED_DETAIL_RE = re.compile(
    r"<strong>\s*Labour Market Impact Assessment \(LMIA\) requested\s*</strong>",
    re.IGNORECASE,
)
NOC_RE = re.compile(r"NOC\s*(\d{4,5})", re.IGNORECASE)
PROPERTY_DESC_RE = re.compile(
    r'<span class="hidden" property="description">(.*?)</span>',
    re.IGNORECASE | re.DOTALL,
)
OVERVIEW_RE = re.compile(
    r"<h3[^>]*>\s*Overview\s*</h3>(.*?)(?:<h3|<h2|</section>)",
    re.IGNORECASE | re.DOTALL,
)
REQUIREMENTS_RE = re.compile(
    r"<h3[^>]*>\s*Job requirements\s*</h3>(.*?)(?:<h3|<h2|</section>)",
    re.IGNORECASE | re.DOTALL,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("canadapt.ingestion.jobbank")


def build_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=MAX_RETRIES,
        connect=MAX_RETRIES,
        read=MAX_RETRIES,
        status=MAX_RETRIES,
        backoff_factor=1.5,
        status_forcelist=RETRYABLE_STATUS_CODES,
        allowed_methods=frozenset({"GET"}),
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update(
        {
            "User-Agent": (
                "CanAdaptBot/1.0 (+https://github.com/GabFontes7/Canadapt-Project; "
                "academic medalhao pipeline; respectful crawl)"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-CA,en;q=0.9",
        }
    )
    return session


def _strip_html(value: str) -> str:
    return plain_text(unescape(value or ""))


def parse_search_card(posting_id: str, block: str) -> dict[str, Any] | None:
    title = TITLE_RE.search(block)
    if not title:
        return None
    title_text = _strip_html(title.group(1))
    if not title_text:
        return None

    job_number_match = JOB_NUMBER_RE.search(block)
    job_number = job_number_match.group(1) if job_number_match else posting_id

    signals: list[str] = []
    if LMIA_REQUESTED_RE.search(block) or re.search(
        r">\s*LMIA requested\s*<", block, re.IGNORECASE
    ):
        signals.extend(["lmia", "lmia_requested"])
    if LMIA_APPROVED_RE.search(block) or re.search(
        r">\s*Approved LMIA\s*<", block, re.IGNORECASE
    ):
        signals.extend(["lmia", "lmia_approved"])

    return {
        "id": posting_id,
        "job_number": job_number,
        "title": title_text,
        "company": _strip_html(BUSINESS_RE.search(block).group(1))
        if BUSINESS_RE.search(block)
        else None,
        "location": _strip_html(LOCATION_RE.search(block).group(1))
        if LOCATION_RE.search(block)
        else None,
        "salary_text": _strip_html(SALARY_RE.search(block).group(1))
        if SALARY_RE.search(block)
        else None,
        "posted_date": _strip_html(DATE_RE.search(block).group(1))
        if DATE_RE.search(block)
        else None,
        "url": urljoin(JOBBANK_ORIGIN, f"/jobsearch/jobposting/{posting_id}"),
        "card_mobility_signals": sorted(set(signals)),
    }


def parse_search_page(html: str) -> list[dict[str, Any]]:
    jobs: list[dict[str, Any]] = []
    for match in ARTICLE_RE.finditer(html):
        card = parse_search_card(match.group(1), match.group(2))
        if card:
            jobs.append(card)
    return jobs


def parse_detail_page(html: str) -> dict[str, Any]:
    description_parts: list[str] = []
    prop = PROPERTY_DESC_RE.search(html)
    if prop:
        description_parts.append(_strip_html(prop.group(1)))
    overview = OVERVIEW_RE.search(html)
    if overview:
        description_parts.append(_strip_html(overview.group(1)))
    requirements = REQUIREMENTS_RE.search(html)
    if requirements:
        description_parts.append(_strip_html(requirements.group(1)))

    signals: list[str] = []
    # Prefer the explicit posting banner; ignore help/JS copy that mentions LMIA.
    if LMIA_REQUESTED_DETAIL_RE.search(html):
        signals.extend(["lmia", "lmia_requested"])

    noc_match = NOC_RE.search(html)
    return {
        "description": "\n\n".join(part for part in description_parts if part).strip()
        or None,
        "noc_code": noc_match.group(1) if noc_match else None,
        "detail_mobility_signals": sorted(set(signals)),
    }


def fetch_search_query(
    session: requests.Session,
    spec: dict[str, Any],
) -> dict[str, Any]:
    label = spec["label"]
    base_params = dict(spec["params"])
    all_jobs: list[dict[str, Any]] = []
    pages_fetched = 0
    errors: list[str] = []

    logger.info("Fetching Job Bank | query=%s | params=%r", label, base_params)

    for page in range(1, MAX_PAGES_PER_QUERY + 1):
        if page > 1:
            time.sleep(PAUSE_BETWEEN_REQUESTS_SECONDS)
        params = {**base_params, "page": str(page)}
        try:
            response = session.get(
                SEARCH_URL,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            if response.status_code in RETRYABLE_STATUS_CODES:
                raise RuntimeError(f"Job Bank HTTP {response.status_code} for {label} page={page}")
            response.raise_for_status()
        except (requests.RequestException, RuntimeError) as exc:
            errors.append(str(exc))
            logger.error("%s", exc)
            break

        cards = parse_search_page(response.text)
        pages_fetched += 1
        if not cards:
            break
        all_jobs.extend(cards)
        logger.info(
            "%s | page %d | page_results=%d | accumulated=%d",
            label,
            page,
            len(cards),
            len(all_jobs),
        )
        # Job Bank search pages usually return 20–25 cards; stop when short.
        if len(cards) < 15:
            break

    return {
        "label": label,
        "params": base_params,
        "mobility_signals": list(spec["mobility_signals"]),
        "priority": int(spec["priority"]),
        "pages_fetched": pages_fetched,
        "results_fetched": len(all_jobs),
        "results": all_jobs,
        "errors": errors,
    }


def enrich_with_details(
    session: requests.Session,
    jobs: list[dict[str, Any]],
) -> None:
    for index, job in enumerate(jobs):
        if index:
            time.sleep(PAUSE_BETWEEN_REQUESTS_SECONDS)
        posting_id = job["id"]
        try:
            response = session.get(
                DETAIL_URL.format(posting_id=posting_id),
                timeout=REQUEST_TIMEOUT_SECONDS,
            )
            response.raise_for_status()
            detail = parse_detail_page(response.text)
        except (requests.RequestException, RuntimeError) as exc:
            logger.warning("Detail fetch failed for %s: %s", posting_id, exc)
            job["detail_error"] = str(exc)
            continue
        job["description"] = detail.get("description")
        job["noc_code"] = detail.get("noc_code")
        detail_signals = detail.get("detail_mobility_signals") or []
        job["canadapt_mobility_signals"] = sorted(
            set(job.get("canadapt_mobility_signals") or []) | set(detail_signals)
        )


def fetch_jobbank_jobs(session: requests.Session) -> dict[str, Any]:
    blocks: list[dict[str, Any]] = []
    for index, spec in enumerate(QUERY_SPECS):
        if index:
            time.sleep(PAUSE_BETWEEN_REQUESTS_SECONDS)
        blocks.append(fetch_search_query(session, spec))

    # Prefer LMIA-tagged listings; fill remaining capacity with international.
    selected: dict[str, dict[str, Any]] = {}
    rejected_cap = 0
    for priority in (0, 1):
        for block in blocks:
            if block["priority"] != priority:
                continue
            for job in block["results"]:
                posting_id = str(job.get("id") or "")
                if not posting_id:
                    continue
                if posting_id in selected:
                    existing = selected[posting_id]
                    existing["canadapt_queries"] = sorted(
                        set(existing.get("canadapt_queries") or []) | {block["label"]}
                    )
                    existing["canadapt_mobility_signals"] = sorted(
                        set(existing.get("canadapt_mobility_signals") or [])
                        | set(block["mobility_signals"])
                        | set(job.get("card_mobility_signals") or [])
                    )
                    continue
                if len(selected) >= MAX_JOBS_PER_RUN:
                    rejected_cap += 1
                    continue
                selected[posting_id] = {
                    **job,
                    "canadapt_queries": [block["label"]],
                    "canadapt_mobility_signals": sorted(
                        set(block["mobility_signals"])
                        | set(job.get("card_mobility_signals") or [])
                    ),
                    "canadapt_filter_version": JOBBANK_FILTER_VERSION,
                }

    kept = list(selected.values())
    if FETCH_DETAILS and kept:
        logger.info("Fetching Job Bank detail pages for %d jobs…", len(kept))
        enrich_with_details(session, kept)

    per_query = {
        block["label"]: sum(
            1 for job in kept if block["label"] in (job.get("canadapt_queries") or [])
        )
        for block in blocks
    }
    logger.info(
        "Job Bank fetch complete | fetched=%d | kept=%d | capped=%d | per_query=%s",
        sum(block["results_fetched"] for block in blocks),
        len(kept),
        rejected_cap,
        per_query,
    )
    return {
        "queries": [
            {
                "label": block["label"],
                "params": block["params"],
                "pages_fetched": block["pages_fetched"],
                "results_fetched": block["results_fetched"],
                "results_kept": per_query.get(block["label"], 0),
                "errors": block["errors"],
            }
            for block in blocks
        ],
        "query_errors": [
            {"query": block["label"], "error": error}
            for block in blocks
            for error in block["errors"]
        ],
        "results_fetched": sum(block["results_fetched"] for block in blocks),
        "results_capped": rejected_cap,
        "results_unique": len(kept),
        "results": kept,
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
    filename = f"jobbank_raw_{now.strftime('%H%M%S')}.json"
    path = out_dir / filename
    path.write_text(json.dumps(document, ensure_ascii=False, indent=2), encoding="utf-8")
    return path, f"bronze/jobbank/{'/'.join(parts)}/{filename}"


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
        raise RuntimeError(f"S3 Job Bank upload failed ({code}): {exc}") from exc
    return f"s3://{bucket}/{s3_key}"


def main() -> int:
    now_local = datetime.now().astimezone()
    extracted_at = datetime.now(timezone.utc)
    try:
        with build_session() as session:
            payload = fetch_jobbank_jobs(session)
        if not payload["results"]:
            raise RuntimeError(
                "Job Bank returned zero mobility-tagged jobs after filtering."
            )
        document = {
            "source": "jobbank",
            "country": "ca",
            "extracted_at_utc": extracted_at.isoformat(),
            "filter_contract": {
                "mobility_required": True,
                "sources": ["lmia_requested", "lmia_approved", "international_candidates"],
                "max_age_param": MAX_AGE_DAYS_PARAM,
                "version": JOBBANK_FILTER_VERSION,
            },
            "queries": payload["queries"],
            "errors": payload["query_errors"],
            "payload": payload,
        }
        local_path, s3_key = save_bronze(document, now_local)
        s3_uri = upload_bronze(document, s3_key)
        logger.info(
            "Job Bank Bronze complete | jobs=%d | local=%s | s3=%s",
            payload["results_unique"],
            local_path,
            s3_uri,
        )
        return 0
    except (ConnectionError, RuntimeError, OSError, EnvironmentError) as exc:
        logger.error("Job Bank ingestion failed: %s", exc)
        return 1


if __name__ == "__main__":
    sys.exit(main())
