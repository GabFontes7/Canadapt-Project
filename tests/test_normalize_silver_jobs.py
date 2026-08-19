"""Normalize mixed Silver job parquet types so DuckDB can union partitions."""

from __future__ import annotations

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
MODULE_PATH = ROOT / "src" / "processing" / "normalize_silver_jobs.py"


def _load():
    spec = importlib.util.spec_from_file_location("normalize_silver_jobs", MODULE_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    sys.modules["normalize_silver_jobs"] = module
    spec.loader.exec_module(module)
    return module


normalize = _load()


class NormalizeSilverJobsTests(unittest.TestCase):
    def test_uuid_and_varchar_run_ids_union_after_normalize(self) -> None:
        root = Path(tempfile.mkdtemp())
        uuid_path = root / "year=2026" / "month=08" / "day=17" / "jobs_clean.parquet"
        varchar_path = root / "year=2026" / "month=08" / "day=19" / "jobs_clean.parquet"
        uuid_path.parent.mkdir(parents=True)
        varchar_path.parent.mkdir(parents=True)

        con = duckdb.connect(":memory:")
        uuid_sql = uuid_path.as_posix().replace("'", "''")
        varchar_sql = varchar_path.as_posix().replace("'", "''")
        glob_sql = (root.as_posix() + "/year=*/month=*/day=*/jobs_clean.parquet").replace(
            "'", "''"
        )
        con.execute(
            f"copy (select uuid() as pipeline_run_id, 1 as job_id) "
            f"to '{uuid_sql}' (format parquet)"
        )
        con.execute(
            f"copy (select 'gha-32032364053-1' as pipeline_run_id, 2 as job_id) "
            f"to '{varchar_sql}' (format parquet)"
        )
        normalize.normalize_file(con, uuid_path)
        normalize.normalize_file(con, varchar_path)
        uuid_type = con.execute(
            "select typeof(pipeline_run_id) from read_parquet(?)",
            [str(uuid_path)],
        ).fetchone()[0]
        varchar_type = con.execute(
            "select typeof(pipeline_run_id) from read_parquet(?)",
            [str(varchar_path)],
        ).fetchone()[0]
        self.assertEqual(uuid_type.lower(), "varchar")
        self.assertEqual(varchar_type.lower(), "varchar")
        count, distinct_types = con.execute(
            f"""
            select count(*), count(distinct typeof(pipeline_run_id))
            from read_parquet(
                '{glob_sql}',
                hive_partitioning=true,
                union_by_name=true
            )
            """
        ).fetchone()
        self.assertEqual(count, 2)
        self.assertEqual(distinct_types, 1)


if __name__ == "__main__":
    unittest.main()
