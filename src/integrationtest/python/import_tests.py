import unittest
import tempfile
import shutil
import json
import os
import neumann.workflows
import luigi
from mock import LocalFileTargetMock
from neumann.core.db import neo4j
from neumann.utils import config
from luigi.mock import MockTarget


class TaskDownloadRecordMocked(neumann.workflows.TaskDownloadRecord):

    def output(self):
        return LocalFileTargetMock('/tmp/download.json')


class TaskImportRecordIntoNeo4jMocked(neumann.workflows.TaskImportRecordIntoNeo4j):

    def requires(self):
        return TaskDownloadRecordMocked(self.date, self.hour, self.tenant)

    def output(self):
        return LocalFileTargetMock('/tmp/neo4j.json')


class LuigiImportTaskTest(unittest.TestCase):

    def setUp(self):
        MockTarget.fs.clear()
        if os.path.exists('/var/neumann/data'):
            shutil.rmtree('/var/neumann/data')

    def test_download_record(self):
        task = TaskDownloadRecordMocked(date='2015-09-01', hour='1', tenant='SOUKAIMY')
        luigi.build([task], local_scheduler=True)
        json_result = json.loads(MockTarget.fs.get_data('/tmp/download.json').decode('utf-8'))

        # Make sure if task output is correct
        self.assertEqual('/var/neumann/data/tmp/2015-09-01-01-SOUKAIMY.gz', json_result['records'][0]['path'])
        self.assertEqual('trackings/records/2015/09/01/2015-09-01-01-SOUKAIMY.gz', json_result['records'][0]['uri'])
        self.assertEqual(200, json_result['records'][0]['s3']['status'])
        self.assertEqual('PROCESSED', json_result['status'])

        # Check if file is downloaded
        self.assertTrue(os.path.exists(json_result['records'][0]['path']))
        shutil.rmtree(tempfile.gettempdir())

    def test_import_record(self):
        # Clear all data for SOUKAIMY in database. But makesure this happens if neo4j is localhost to avoid
        # misconfiguration problem.
        if config.get('neo4j', 'host') != '127.0.0.1':
            self.fail('This test only works if neo4j host is configured to 127.0.0.1')
        neo4j.run_query(neo4j.Query('MATCH (n:SOUKAIMY) OPTIONAL MATCH (n)-[r]-() DELETE n,r', None))

        # Execute task
        task = TaskImportRecordIntoNeo4jMocked(date='2015-09-01', hour='1', tenant='SOUKAIMY')
        luigi.build([task], local_scheduler=True)

        # Check database if records are imported
        result = neo4j.run_query(neo4j.Query('MATCH (n:SOUKAIMY) RETURN COUNT(n)', None))
        self.assertEqual(341, result.get_response()['data'][0][0])

        # Check if output file is created
        json_result = json.loads(MockTarget.fs.get_data('/tmp/neo4j.json').decode('utf-8'))
        self.assertEqual('Records Imported for [tenant=SOUKAIMY, date=2015-09-01, hour=1]: 1', json_result['message'])
