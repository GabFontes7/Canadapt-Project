"""Tests for lake retention, hive keys and Gemini degradation heuristics."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "src" / "processing" / "pipeline_policy.py"


def _load(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


policy = _load("pipeline_policy", POLICY_PATH)


class PipelinePolicyTests(unittest.TestCase):
    def test_hive_prefixes_cover_bronze_and_silver(self) -> None:
        prefixes = policy.hive_prefixes_for_day(date(2026, 8, 17))
        self.assertTrue(any(p.startswith("bronze/year=2026/") for p in prefixes))
        self.assertTrue(any("silver/jobs/" in p for p in prefixes))
        self.assertTrue(all("/year=2026/month=08/day=17/" in p for p in prefixes))

    def test_retained_window_length(self) -> None:
        prefixes = policy.retained_hive_prefixes(as_of=date(2026, 8, 19), days=7)
        unique_days = {
            "/".join(
                part
                for part in p.split("/")
                if part.startswith(("year=", "month=", "day="))
            )
            for p in prefixes
        }
        self.assertEqual(len(unique_days), 7)

    def test_durable_prefixes_keep_caches_and_wages(self) -> None:
        durable = policy.durable_prefixes()
        self.assertIn("silver/metadata/", durable)
        self.assertIn("silver/wages/", durable)
        self.assertNotIn("gold/", durable)

    def test_gemini_unavailable_detects_503(self) -> None:
        self.assertTrue(
            policy.is_gemini_unavailable(
                RuntimeError("503 UNAVAILABLE. high demand")
            )
        )
        self.assertFalse(policy.is_gemini_unavailable(RuntimeError("invalid json")))

    def test_restore_filters_old_hive_keys(self) -> None:
        cutoff = date(2026, 8, 1)
        old_key = "silver/jobs/year=2026/month=06/day=01/jobs_clean.parquet"
        new_key = "silver/jobs/year=2026/month=08/day=17/jobs_clean.parquet"
        self.assertLess(policy.hive_date_from_key(old_key), cutoff)
        self.assertGreaterEqual(policy.hive_date_from_key(new_key), cutoff)
        self.assertIsNone(policy.hive_date_from_key("silver/metadata/geo_cache.json"))


if __name__ == "__main__":
    unittest.main()
