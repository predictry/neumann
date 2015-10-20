import unittest
from neumann.message.task import TrimDataTask, ImportRecordTask, ComputeRecommendationTask, SyncItemStoreTask


class TaskTest(unittest.TestCase):

    def test_trim_data(self):
        trim_data = TrimDataTask()
        valid_payload = {
            "type": "trim-data",
            "payload": {
                "tenant": "tenant1",
                "date": "2015-12-01",
                "startingDate": "2015-12-01",
                "period": 30
            }
        }
        invalid_payload = {
            "type": "tapirus",
            "user": "unknown"
        }
        self.assertTrue(trim_data.can_parse(valid_payload))
        self.assertFalse(trim_data.can_parse(invalid_payload))
        trim_data.parse(valid_payload)
        with self.assertRaises(ValueError):
            trim_data.parse(invalid_payload)

    def test_compute_recommendation(self):
        compute_recommendation = ComputeRecommendationTask()
        valid_payload = {
            "type": "compute-recommendation",
            "payload": {
                "tenant": "tenant1",
                "date": "2015-12-01",
                "algorithm": {
                    "name": "algo-1",
                    "params": {
                        "param1": "value1",
                        "param2": "value2"
                    }
                },
                "output": {
                    "type": "s3",
                    "bucket": "exercises",
                    "prefix": "photos/2006/January/sample.jpg",
                    "region": "eu-west-1"
                }
            }
        }
        invalid_payload = {
            "type": "test-recommendation",
            "payload": {
                "tenant": "tenant1",
                "date": "2015-12-01",
                "algorithm": {
                    "name": "algo-1",
                    "params": {
                        "param1": "value1",
                        "param2": "value2"
                    }
                },
                "output": {
                    "type": "s3",
                    "bucket": "exercises",
                }
            }
        }
        self.assertTrue(compute_recommendation.can_parse(valid_payload))
        self.assertFalse(compute_recommendation.can_parse(invalid_payload))
        compute_recommendation.parse(valid_payload)
        with self.assertRaises(ValueError):
            compute_recommendation.parse(invalid_payload)

    def test_import_record(self):
        import_record = ImportRecordTask()
        valid_payload = {
            "type": "import-record",
            "payload": {
                "tenant": "tenant1",
                "date": "2015-12-01",
                "hour": 10,
                "input": {
                    "type": "tapirus",
                    "host": "192.168.0.100",
                    "port": 1999
                }
            }
        }
        invalid_payload = {
            "type": "test-recommendation",
            "payload": {
                "tenant": "tenant1",
                "date": "2015-12-01"
            }
        }
        self.assertTrue(import_record.can_parse(valid_payload))
        self.assertFalse(import_record.can_parse(invalid_payload))
        import_record.parse(valid_payload)
        with self.assertRaises(ValueError):
            import_record.parse(invalid_payload)

    def test_sync_item_store(self):
        sync_item_store = SyncItemStoreTask()
        valid_payload = {
            "type": "sync-item-store",
            "payload": {
                "tenant": "tenant1",
                "target": {
                    "type": "s3",
                    "bucket": "exercises",
                    "prefix": "photos/2006/January/sample.jpg",
                    "region": "eu-west-1"
                }
            }
        }
        invalid_payload = {
            "type": "test-sync",
            "payload": {
                "tenant": "tenant1",
                "date": "2015-12-01"
            }
        }
        self.assertTrue(sync_item_store.can_parse(valid_payload))
        self.assertFalse(sync_item_store.can_parse(invalid_payload))
        sync_item_store.parse(valid_payload)
        with self.assertRaises(ValueError):
            sync_item_store.parse(invalid_payload)
