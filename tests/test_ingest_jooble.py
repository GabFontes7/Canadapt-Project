"""Unit tests for Jooble's strict scope filter."""

from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "src" / "ingestion" / "ingest_jooble.py"
)
SPEC = importlib.util.spec_from_file_location("ingest_jooble", MODULE_PATH)
assert SPEC and SPEC.loader
ingest_jooble = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ingest_jooble)


def job(title: str, snippet: str) -> dict:
    return {
        "id": "123",
        "title": title,
        "snippet": snippet,
        "company": "Example",
        "link": "https://example.com/job/123",
        "location": "Toronto",
        "type": "Full-time",
    }


class JoobleScopeTests(unittest.TestCase):
    def test_keeps_technology_with_explicit_sponsorship(self) -> None:
        result = ingest_jooble.filter_job(
            job("Senior Software Developer", "Visa sponsorship is available."),
            "technology",
        )
        self.assertEqual(result["canadapt_area"], "technology")
        self.assertIn("visa_sponsorship", result["canadapt_mobility_signals"])

    def test_keeps_banking_back_office_with_relocation(self) -> None:
        result = ingest_jooble.filter_job(
            job(
                "Banking Back Office Analyst",
                "International relocation assistance to Canada is provided.",
            ),
            "banking_operations",
        )
        self.assertEqual(result["canadapt_area"], "banking_operations")
        self.assertIn("relocation", result["canadapt_mobility_signals"])

    def test_rejects_allowed_area_without_mobility(self) -> None:
        result = ingest_jooble.filter_job(
            job("Data Engineer", "Build reliable analytics pipelines."),
            "technology",
        )
        self.assertIsNone(result)

    def test_rejects_unapproved_professional_area(self) -> None:
        result = ingest_jooble.filter_job(
            job("Registered Nurse", "LMIA support may be provided."),
            "technology",
        )
        self.assertIsNone(result)

    def test_rejects_area_mismatch(self) -> None:
        result = ingest_jooble.filter_job(
            job("Cloud Engineer", "Work permit sponsorship provided."),
            "banking_operations",
        )
        self.assertIsNone(result)

    def test_rejects_explicit_no_sponsorship(self) -> None:
        result = ingest_jooble.filter_job(
            job(
                "Senior Data Engineer",
                "Please note that <b>visa sponsorship </b>is not provided for this role.",
            ),
            "technology",
        )
        self.assertIsNone(result)

    def test_rejects_existing_work_authorization_requirement(self) -> None:
        result = ingest_jooble.filter_job(
            job(
                "AML Operations Analyst",
                "Candidates must already be legally authorized to work in Canada.",
            ),
            "banking_operations",
        )
        self.assertIsNone(result)

    def test_rejects_domestic_relocation_for_canadian_residents(self) -> None:
        result = ingest_jooble.filter_job(
            job(
                "FPGA Design Engineer",
                "Candidates must be Canadian residents. Relocation assistance available.",
            ),
            "technology",
        )
        self.assertIsNone(result)


if __name__ == "__main__":
    unittest.main()
