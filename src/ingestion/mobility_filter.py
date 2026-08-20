"""Shared mobility text filters for Adzuna and Jooble ingestion."""

from __future__ import annotations

import json
import re
from html import unescape
from typing import Any

FILTER_VERSION = "mobility_text_v2"
ADZUNA_FILTER_VERSION = "adzuna_mobility_text_v2"
JOOBLE_FILTER_VERSION = "jooble_area_mobility_v1"

MOBILITY_PATTERNS: dict[str, re.Pattern[str]] = {
    "visa_sponsorship": re.compile(
        r"\b(?:visa sponsorship|sponsor(?:ing|ship)? (?:a |the )?visa|"
        r"work visa sponsorship|sponsorship available|parrainage de visa|"
        r"employer (?:will )?sponsor(?:ing|ship)?)\b",
        re.IGNORECASE,
    ),
    "lmia": re.compile(r"\b(?:lmia|eimt)\b", re.IGNORECASE),
    "relocation": re.compile(
        r"\b(?:relocation (?:support|assistance|package|benefit)|"
        r"relocate to canada|international relocation|"
        r"aide au déménagement (?:international|vers le canada))\b",
        re.IGNORECASE,
    ),
    "work_permit": re.compile(
        r"\b(?:work permit (?:support|sponsorship|provided|assistance)|"
        r"support (?:for |with )?(?:a )?work permit|permis de travail)\b",
        re.IGNORECASE,
    ),
    "immigration_support": re.compile(
        r"\b(?:immigration support|immigration assistance|"
        r"support (?:with |for )?(?:your )?immigration)\b",
        re.IGNORECASE,
    ),
}

NEGATIVE_MOBILITY_PATTERN = re.compile(
    r"\b(?:"
    r"(?:visa )?sponsorship (?:is |will be )?not (?:available|offered|provided)|"
    r"(?:do|does|will) not (?:offer|provide|support|sponsor)|"
    r"(?:not|unable|not able) to (?:offer|provide|support|sponsor)|"
    r"no (?:visa |work permit )?sponsorship|"
    r"without (?:current or future )?(?:visa )?sponsorship|"
    r"cannot sponsor|"
    r"must (?:already )?be (?:legally )?(?:authorized|eligible) to work|"
    r"(?:must|need to) (?:be |have )?(?:legally )?(?:authorized|eligible) to work|"
    r"eligible to work in canada|"
    r"right to work in canada|"
    r"currently (?:authorized|eligible) to work|"
    r"(?:candidates? )?must be (?:a )?canadian "
    r"(?:citizens?|residents?|permanent residents?)|"
    r"only (?:canadian )?(?:citizens?|residents?|permanent residents?)|"
    r"relocation (?:is )?not (?:available|offered|provided)|"
    r"no relocation (?:assistance|support|package)"
    r")\b",
    re.IGNORECASE,
)

MOBILITY_LABELS = {
    "visa_sponsorship": "Patrocínio de visto",
    "lmia": "LMIA/EIMT",
    "lmia_requested": "LMIA solicitado (Job Bank)",
    "lmia_approved": "LMIA aprovado (Job Bank)",
    "international_candidates": "Aberto a internacionais (Job Bank)",
    "relocation": "Relocação",
    "work_permit": "Work permit",
    "immigration_support": "Apoio à imigração",
}


def plain_text(value: str) -> str:
    """Strip markup and collapse whitespace before regex matching."""
    without_tags = re.sub(r"<[^>]+>", " ", unescape(value or ""))
    return re.sub(r"\s+", " ", without_tags).strip()


def mobility_signals(text: str) -> list[str]:
    """Return mobility signal keys found in listing text."""
    return [
        label for label, pattern in MOBILITY_PATTERNS.items() if pattern.search(text)
    ]


def has_negative_mobility(text: str) -> bool:
    return bool(NEGATIVE_MOBILITY_PATTERN.search(text))


def parse_mobility_signals(value: object) -> list[str]:
    """Normalize mobility signals stored as list or JSON string."""
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    text = str(value).strip()
    if not text or text in {"[]", "null"}:
        return []
    if text.startswith("[") and text.endswith("]"):
        try:
            parsed = json.loads(text)
        except json.JSONDecodeError:
            inner = text[1:-1].strip()
            if not inner:
                return []
            return [item.strip().strip("'\"") for item in inner.split(",") if item.strip()]
        else:
            if isinstance(parsed, list):
                return [str(item) for item in parsed if str(item).strip()]
            return []
    return [text]


def mobility_confirmed(value: object) -> bool:
    return bool(parse_mobility_signals(value))


def mobility_signal_labels(value: object) -> list[str]:
    return [
        MOBILITY_LABELS.get(key, key.replace("_", " "))
        for key in parse_mobility_signals(value)
    ]


def filter_adzuna_job(job: dict[str, Any]) -> dict[str, Any] | None:
    """Keep Adzuna listings only when mobility is explicit in the posting text."""
    company = (job.get("company") or {}).get("display_name") or ""
    category = (job.get("category") or {}).get("label") or ""
    text = plain_text(
        " ".join(
            str(job.get(field) or "")
            for field in ("title", "description")
        )
        + " "
        + plain_text(f"{company} {category}")
    )
    signals = mobility_signals(text)
    if not signals or has_negative_mobility(text):
        return None
    if not job.get("id") or not job.get("title") or not job.get("redirect_url"):
        return None
    return {
        **job,
        "canadapt_mobility_signals": signals,
        "canadapt_filter_version": ADZUNA_FILTER_VERSION,
    }
