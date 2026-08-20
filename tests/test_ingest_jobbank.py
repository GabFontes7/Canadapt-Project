"""Tests for Job Bank HTML parsing helpers."""

from __future__ import annotations

import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "src" / "ingestion" / "ingest_jobbank.py"


def _load():
    spec = importlib.util.spec_from_file_location("ingest_jobbank", MODULE_PATH)
    assert spec and spec.loader
    # Ensure mobility_filter import resolves.
    sys.path.insert(0, str(ROOT / "src" / "ingestion"))
    module = importlib.util.module_from_spec(spec)
    sys.modules["ingest_jobbank"] = module
    spec.loader.exec_module(module)
    return module


jb = _load()

SAMPLE_ARTICLE = """
<article id="article-50112293" class="action-buttons"><a href="/jobsearch/jobposting/50112293" class="resultJobItem">
  <h3 class="title">
    <span class="flag">
      <span class="jobLMIAflag submitted nopopup">LMIA requested</span>
    </span>
    <span class="noctitle"> general labourer - farm </span>
  </h3>
  <ul class="list-unstyled">
    <li class="date">August 19, 2026</li>
    <li class="business">Mark Craig Inc.</li>
    <li class="location"><span class="wb-inv">Location</span> Albany (PE)</li>
    <li class="salary">Salary $17.00 hourly</li>
    <li class="source"><span class="wb-inv">Job number:</span><span class="fa fa-hashtag"></span>3651491</li>
  </ul></a>
</article>
"""

SAMPLE_DETAIL = """
<span class="hidden" property="description">Pack fruits and vegetables. Clean work area.</span>
<h3>Overview</h3><div>Languages English. Rural area.</div>
<span class="noc-no">NOC 85101</span>
<p><strong>Labour Market Impact Assessment (LMIA) requested</strong></p>
<script>var approve = "This employer has an approved Labour Market Impact Assessment (LMIA)";</script>
"""


class JobBankParseTests(unittest.TestCase):
    def test_parse_search_page_extracts_lmia_card(self) -> None:
        cards = jb.parse_search_page(SAMPLE_ARTICLE)
        self.assertEqual(len(cards), 1)
        card = cards[0]
        self.assertEqual(card["id"], "50112293")
        self.assertEqual(card["title"], "general labourer - farm")
        self.assertEqual(card["company"], "Mark Craig Inc.")
        self.assertIn("Albany", card["location"])
        self.assertIn("lmia_requested", card["card_mobility_signals"])

    def test_parse_detail_page_extracts_noc_and_description(self) -> None:
        detail = jb.parse_detail_page(SAMPLE_DETAIL)
        self.assertEqual(detail["noc_code"], "85101")
        self.assertIn("Pack fruits", detail["description"] or "")
        self.assertIn("lmia_requested", detail["detail_mobility_signals"])
        self.assertNotIn("lmia_approved", detail["detail_mobility_signals"])


if __name__ == "__main__":
    unittest.main()
