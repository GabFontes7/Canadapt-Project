"""Tests for shared mobility filters."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "src" / "ingestion" / "mobility_filter.py"


def _load():
    spec = importlib.util.spec_from_file_location("mobility_filter", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["mobility_filter"] = module
    spec.loader.exec_module(module)
    return module


mobility = _load()


class MobilityFilterTests(unittest.TestCase):
    def test_keeps_adzuna_with_visa_sponsorship(self) -> None:
        job = {
            "id": "1",
            "title": "Data Engineer",
            "redirect_url": "https://example.com/1",
            "description": "We offer visa sponsorship for qualified candidates.",
            "company": {"display_name": "Acme"},
        }
        kept = mobility.filter_adzuna_job(job)
        self.assertIsNotNone(kept)
        assert kept is not None
        self.assertIn("visa_sponsorship", kept["canadapt_mobility_signals"])

    def test_rejects_adzuna_without_mobility_text(self) -> None:
        job = {
            "id": "2",
            "title": "Software Developer",
            "redirect_url": "https://example.com/2",
            "description": "Build APIs for our Toronto office.",
            "company": {"display_name": "Local Co"},
        }
        self.assertIsNone(mobility.filter_adzuna_job(job))

    def test_rejects_local_authorization_requirement(self) -> None:
        job = {
            "id": "3",
            "title": "Analyst",
            "redirect_url": "https://example.com/3",
            "description": (
                "Visa sponsorship is not available. "
                "Candidates must be eligible to work in Canada."
            ),
            "company": {"display_name": "Bank"},
        }
        self.assertIsNone(mobility.filter_adzuna_job(job))

    def test_parse_mobility_signals_from_json(self) -> None:
        self.assertEqual(
            mobility.parse_mobility_signals('["lmia", "relocation"]'),
            ["lmia", "relocation"],
        )
        self.assertFalse(mobility.mobility_confirmed("[]"))


if __name__ == "__main__":
    unittest.main()
