"""Portal helpers for mobility signal labels (no ingestion dependency)."""

from __future__ import annotations

import json

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


def parse_mobility_signals(value: object) -> list[str]:
    """Normalize mobility signals stored as list or JSON/DuckDB string."""
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
            return [
                item.strip().strip("'\"") for item in inner.split(",") if item.strip()
            ]
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
