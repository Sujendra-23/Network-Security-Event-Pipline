import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

from jobs.retain_delta import PROPERTIES, audit_path, read_audit, retain_delta


class RetentionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for name in ('network_events', 'label_counts'):
            write_deltalake(str(self.root / name), pa.table({'label': ['BENIGN']}))

    def test_vacuum_removes_expired_file_preserves_current_and_recent(self):
        path = self.root / 'network_events'
        old = set(path.glob('*.parquet'))
        write_deltalake(str(path), pa.table({'label': ['ATTACK']}), mode='overwrite')
        # Age only a synthetic fixture tombstone; production safety stays enabled.
        log = path / '_delta_log' / '00000000000000000001.json'
        actions = [json.loads(line) for line in log.read_text().splitlines()]
        for action in actions:
            if 'remove' in action:
                action['remove']['deletionTimestamp'] = 0
        log.write_text('\n'.join(json.dumps(action) for action in actions) + '\n')
        recent = set(path.glob('*.parquet')) - old
        write_deltalake(str(path), pa.table({'label': ['CURRENT']}), mode='overwrite')
        report = retain_delta(str(self.root), 'test')
        attempt = report['tables']['network_events']['attempts'][0]
        self.assertEqual(attempt['removed_file_count'], 1)
        self.assertTrue(all(not p.exists() for p in old))
        self.assertTrue(all(p.exists() for p in recent))
        self.assertEqual(DeltaTable(str(path)).to_pyarrow_table()['label'].to_pylist(), ['CURRENT'])
        self.assertEqual(read_audit(str(self.root), 'test')['status'], 'complete')
        self.assertEqual(DeltaTable(str(path)).metadata().configuration, PROPERTIES)
        second = retain_delta(str(self.root), 'test')
        attempts = second['tables']['network_events']['attempts']
        self.assertEqual(len(attempts), 2)
        self.assertEqual(attempts[0]['removed_file_count'], 1)
        self.assertEqual(attempts[1]['removed_file_count'], 0)

    def test_noop_and_optional_tables(self):
        report = retain_delta(str(self.root), 'run')
        self.assertFalse(report['tables']['src_ip_window_counts']['present'])
        self.assertEqual(report['tables']['label_counts']['attempts'][0]['removed_file_count'], 0)
        with self.assertRaises(FileNotFoundError):
            read_audit(str(self.root), 'other-run')

    def test_failure_is_audited_and_cannot_be_published(self):
        with patch.object(DeltaTable, 'vacuum', side_effect=RuntimeError('disk failure')):
            with self.assertRaisesRegex(RuntimeError, 'disk failure'):
                retain_delta(str(self.root), 'failure')
        self.assertEqual(json.loads(audit_path(str(self.root), 'failure').read_text())['status'], 'failed')
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            read_audit(str(self.root), 'failure')

    def test_missing_required_table_fails(self):
        with self.assertRaises(FileNotFoundError):
            retain_delta(str(self.root / 'missing'), 'run')


if __name__ == '__main__':
    unittest.main()
