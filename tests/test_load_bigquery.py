import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pyarrow as pa
import pyarrow.parquet as pq
from deltalake import write_deltalake
from jobs.load_bigquery import export_snapshot, load_bigquery, TABLES


class BigQueryLoadTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.data = pa.table({"event_date": [date(2017, 7, 3)], "label": ["BENIGN"], "flow_count": [2]})
        for name in TABLES:
            write_deltalake(str(self.root / name), self.data, partition_by=["event_date"])
        self.client = Mock()
        self.client.create_dataset.return_value = SimpleNamespace(location="US")
        self.client.load_table_from_file.side_effect = self.upload
        self.uploaded = {}

    def upload(self, stream, target, **kwargs):
        data = pq.read_table(stream)
        self.uploaded[target] = (data, kwargs["job_config"].to_api_repr())
        return SimpleNamespace(output_rows=data.num_rows, job_id="test-job", result=lambda: None)

    def run_load(self):
        return load_bigquery(str(self.root), "test-project", "security", "US", self.client)

    def test_current_snapshot_excludes_obsolete_files_and_restores_partition_column(self):
        write_deltalake(str(self.root / "network_events"), self.data.slice(0, 0), mode="overwrite")
        output = self.root / "export.parquet"
        version, count = export_snapshot(self.root / "network_events", output)
        self.assertEqual((version, count), (1, 0))
        self.assertIn("event_date", pq.read_schema(output).names)
        self.assertTrue(list((self.root / "network_events").rglob("*.parquet")))

    def test_all_tables_partitioned_clustered_and_replaced(self):
        report = self.run_load()
        self.assertEqual(len(report), 4)
        for data, config in self.uploaded.values():
            self.assertEqual(data["event_date"].to_pylist(), [date(2017, 7, 3)])
            self.assertEqual(config["load"]["timePartitioning"], { "type": "DAY", "expirationMs": "5184000000"})
            self.assertEqual(config["load"]["clustering"]["fields"], ["label"])
            self.assertEqual(config["load"]["writeDisposition"], "WRITE_TRUNCATE")

    def test_failure_propagates(self):
        self.client.load_table_from_file.return_value = None
        self.client.load_table_from_file.side_effect = RuntimeError("upload failed")
        with self.assertRaisesRegex(RuntimeError, "upload failed"):
            self.run_load()

    def test_row_count_mismatch_fails(self):
        self.client.load_table_from_file.side_effect = None
        self.client.load_table_from_file.return_value = SimpleNamespace(output_rows=0, result=lambda: None)
        with self.assertRaisesRegex(RuntimeError, "expected 1 rows"):
            self.run_load()

    def test_missing_required_table_fails_before_cloud_calls(self):
        with self.assertRaises(FileNotFoundError):
            load_bigquery(str(self.root / "missing"), "test-project", "security", "US", self.client)
        self.client.create_dataset.assert_not_called()

    def test_missing_optional_table_removes_stale_destination(self):
        import shutil
        shutil.rmtree(self.root / "src_ip_window_counts")
        self.assertFalse(self.run_load()["src_ip_window_counts"]["present"])
        self.client.delete_table.assert_called_once_with("test-project.security.src_ip_window_counts", not_found_ok=True)

    def test_old_window_schema_requires_transform(self):
        write_deltalake(str(self.root / "src_ip_window_counts"), self.data.drop(["label"]), mode="overwrite", schema_mode="overwrite")
        with self.assertRaisesRegex(ValueError, "rerun spark_transform"):
            self.run_load()


if __name__ == "__main__":
    unittest.main()
