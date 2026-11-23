import csv
from pathlib import Path
import unittest

ROOT=Path(__file__).resolve().parents[1]
EXPECTED={'datasets.csv': (['dataset_id', 'owner_lab', 'sensitivity', 'release_version', 'share_scope', 'release_state'], 42), 'source_partitions.csv': (['dataset_id', 'partition_key', 'source_version', 'checksum', 'updated_at', 'withdrawal_state'], 100), 'replica_tasks.csv': (['task_id', 'dataset_id', 'region', 'partition_key', 'target_version', 'attempt', 'task_state', 'completed_at'], 120), 'replica_objects.csv': (['task_id', 'object_key', 'checksum', 'metadata_version', 'access_policy_version', 'verified_state'], 120), 'consumer_requests.csv': (['request_id', 'consumer_lab', 'dataset_id', 'region', 'requested_scope', 'requested_at', 'decision', 'snapshot_ref'], 60)}

class MaterialTests(unittest.TestCase):
    def test_csv_contract(self):
        for filename,(columns,count) in EXPECTED.items():
            with self.subTest(filename=filename):
                with (ROOT/"data"/filename).open(encoding="utf-8-sig",newline="") as stream:
                    reader=csv.DictReader(stream); rows=list(reader)
                self.assertEqual(reader.fieldnames,columns)
                self.assertEqual(len(rows),count)
                self.assertTrue(all(None not in row for row in rows))

if __name__=="__main__": unittest.main()
