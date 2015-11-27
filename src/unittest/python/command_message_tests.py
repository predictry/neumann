import unittest
import datetime
from neumann.message.command import CommandMessage
from neumann.message.datasource import S3DataSource, TapirusDataSource
from neumann.message.task import TrimDataTask, ComputeRecommendationTask, ImportRecordTask, SyncItemStoreTask
from neumann.services import DataTrimmingService, RecommendService, RecordImportService
from unittest.mock import patch, call


# noinspection PyUnresolvedReferences
class CommandMessageTests(unittest.TestCase):

    @patch.object(DataTrimmingService, 'trim')
    def test_trim_data(self, service):
        json_message = '''{
            "jobId": "123",
            "type": "trim-data",
            "payload": {
                "tenant": "tenant1",
                "date": "2015-12-01",
                "startingDate": "2015-11-01",
                "period": 30
            }
        }'''
        command = CommandMessage(json_message)
        self.assertEqual(TrimDataTask, command.task.__class__)
        self.assertEqual('trim-data', command.get_type())
        self.assertEqual('tenant1', command.task.tenant)
        self.assertEqual('2015-12-01', command.task.date)
        self.assertEqual('2015-11-01', command.task.startingDate)
        self.assertEqual(30, command.task.period)

        # Test run
        command.execute()
        service.assert_called_one_with(date='2015-12-01', tenant='tenant1', startingDate='2015-11-01', period=30)

    @patch.object(RecommendService, 'compute')
    def test_compute_recommendation(self, service):
        json_message = '''{
            "jobId": "123",
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
        }'''
        command = CommandMessage(json_message)
        self.assertEqual(ComputeRecommendationTask, command.task.__class__)
        self.assertEqual('compute-recommendation', command.get_type())
        self.assertEqual('tenant1', command.task.tenant)
        self.assertEqual('2015-12-01', command.task.date)
        self.assertEqual('algo-1', command.task.algorithm['name'])
        self.assertEqual('value1', command.task.algorithm['params']['param1'])
        self.assertEqual('value2', command.task.algorithm['params']['param2'])
        self.assertEqual(S3DataSource, command.task.output.__class__)
        self.assertEqual('exercises', command.task.output.bucket)
        self.assertEqual('photos/2006/January/sample.jpg', command.task.output.prefix)
        self.assertEqual('eu-west-1', command.task.output.region)

        # Test run
        command.execute()
        service.assert_called_one_with(date='2015-12-01', tenant='tenant1', algorithm='algo-1')

    @patch.object(RecordImportService, 'harvest')
    def test_import_record(self, service):
        json_message = '''{
            "jobId": "123",
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
        }'''
        command = CommandMessage(json_message)
        self.assertEqual(ImportRecordTask, command.task.__class__)
        self.assertEqual('import-record', command.get_type())
        self.assertEqual('tenant1', command.task.tenant)
        self.assertEqual('2015-12-01', command.task.date)
        self.assertEqual(10, command.task.hour)
        self.assertEqual(TapirusDataSource, command.task.input.__class__)
        self.assertEqual('192.168.0.100', command.task.input.host)
        self.assertEqual(1999, command.task.input.port)

        # Test run
        command.execute()
        service.assert_called_one_with(timestamp='2015-12-01-10', tenant='tenant1')

    @patch.object(RecordImportService, 'harvest')
    def test_import_record_day(self, service):
        json_message = '''{
            "jobId": "123",
            "type": "import-record",
            "payload": {
                "tenant": "tenant1",
                "date": "2015-12-01",
                "input": {
                    "type": "tapirus",
                    "host": "192.168.0.100",
                    "port": 1999
                }
            }
        }'''
        command = CommandMessage(json_message)
        self.assertEqual(ImportRecordTask, command.task.__class__)
        self.assertEqual('import-record', command.get_type())
        self.assertEqual('tenant1', command.task.tenant)
        self.assertEqual('2015-12-01', command.task.date)
        self.assertEqual(TapirusDataSource, command.task.input.__class__)
        self.assertEqual('192.168.0.100', command.task.input.host)
        self.assertEqual(1999, command.task.input.port)

        # Test run
        command.execute()
        expected_args = [call(job_id='123', timestamp=datetime.datetime(2015, 12, 1, x, 0), tenant='tenant1')
                         for x in range(0, 24)]
        self.assertEqual(expected_args, service.call_args_list)

    def test_sync_item_store(self):
        json_message = '''{
            "jobId": "123",
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
        }'''
        command = CommandMessage(json_message)
        self.assertEqual(SyncItemStoreTask, command.task.__class__)
        self.assertEqual('sync-item-store', command.get_type())
        self.assertEqual('tenant1', command.task.tenant)
        self.assertEqual(S3DataSource, command.task.target.__class__)
        self.assertEqual('exercises', command.task.target.bucket)
        self.assertEqual('photos/2006/January/sample.jpg', command.task.target.prefix)
        self.assertEqual('eu-west-1', command.task.target.region)
