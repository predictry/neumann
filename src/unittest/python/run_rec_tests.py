import unittest
import csv
import io
import os
import shutil
from unittest.mock import patch
from neumann.core.db import neo4j
import neumann.workflows
import luigi
from luigi.mock import MockTarget
from neumann.utils import config


class LocalFileTargetMock(MockTarget):

    def __init__(self, filename):
        super().__init__(filename)

    def makedirs(self):
        pass


class TaskRetrieveTenantsItemsListMocked(neumann.workflows.TaskRetrieveTenantsItemsList):

    def output(self):
        return LocalFileTargetMock('/tmp/download.json')


class TaskComputeRecommendationsMocked(neumann.workflows.TaskComputeRecommendations):

    def requires(self):
        return TaskRetrieveTenantsItemsListMocked(date=self.date, tenant=self.tenant, id=self.id,
                                                  start=self.start, end=self.end)

    def output(self):
        return LocalFileTargetMock('/tmp/compute.json')


class TaskStoreRecommendationResultsMocked(neumann.workflows.TaskStoreRecommendationResults):

    def requires(self):
        return TaskComputeRecommendationsMocked(date=self.date, tenant=self.tenant, id=self.id,
                                                start=self.start, end=self.end, algorithm=self.algorithm)

    def output(self):
        return LocalFileTargetMock('/tmp/store.json')


class LuigiRunRecommendationTask(unittest.TestCase):

    def setUp(self):
        MockTarget.fs.clear()
        if os.path.exists('/var/neumann/data'):
            shutil.rmtree('/var/neumann/data')

        # Clear all data for SOUKAIMY in database. But makesure this happens if neo4j is localhost to avoid
        # misconfiguration problem.
        if config.get('neo4j', 'host') != '127.0.0.1':
            self.fail('This test only works if neo4j host is configured to 127.0.0.1')
        neo4j.run_query(neo4j.Query('MATCH (n:SOUKAIMY) OPTIONAL MATCH (n)-[r]-() DELETE n,r', None))

        import_task = neumann.workflows.TaskImportRecordIntoNeo4j(date='2015-09-01', hour='1', tenant='SOUKAIMY')
        luigi.build([import_task], local_scheduler=True)

    def test_retrieve_tenants_0(self):
        # Simulating empty database
        if config.get('neo4j', 'host') != '127.0.0.1':
            self.fail('This test only works if neo4j host is configured to 127.0.0.1')
        neo4j.run_query(neo4j.Query('MATCH (n:SOUKAIMY) OPTIONAL MATCH (n)-[r]-() DELETE n,r', None))
        task = TaskRetrieveTenantsItemsListMocked(date='2015-09-01', tenant='SOUKAIMY', start=0, end=10, id=1)
        luigi.build([task], local_scheduler=True)

        # Check the result
        self.assertEqual('"tenant","itemId"\r\n', MockTarget.fs.get_data('/tmp/download.json').decode('utf-8'))

    def test_retrieve_tenants_0_10(self):
        main_task = TaskRetrieveTenantsItemsListMocked(date='2015-09-01', tenant='SOUKAIMY', start=0, end=10, id=1)
        luigi.build([main_task], local_scheduler=True)

        # Check the result
        bio = io.StringIO(MockTarget.fs.get_data('/tmp/download.json').decode('utf-8'))
        result = list(csv.reader(bio))
        expected = [['SOUKAIMY', 'SECM-S0001_00322'], ['SOUKAIMY', 'SECM-S0001_00706'],
                    ['SOUKAIMY', 'SECM-S0002_00030'], ['SOUKAIMY', 'SECM-S0002_00032'],
                    ['SOUKAIMY', 'SECM-S0002_00105'], ['SOUKAIMY', 'SECM-S0005_00119'],
                    ['SOUKAIMY', 'SECM-S0007_00019'], ['SOUKAIMY', 'SECM-S0008_00014'],
                    ['SOUKAIMY', 'SECM-S0012_00025'], ['SOUKAIMY', 'SECM-S0012_00137']]
        self.assertEqual(10, len(result[1:]))
        self.assertListEqual(expected, result[1:])

    def test_retrieve_tenants_0_5(self):
        main_task = TaskRetrieveTenantsItemsListMocked(date='2015-09-01', tenant='SOUKAIMY', start=0, end=5, id=1)
        luigi.build([main_task], local_scheduler=True)

        # Check the result
        bio = io.StringIO(MockTarget.fs.get_data('/tmp/download.json').decode('utf-8'))
        result = list(csv.reader(bio))
        expected = [['SOUKAIMY', 'SECM-S0001_00322'], ['SOUKAIMY', 'SECM-S0001_00706'],
                    ['SOUKAIMY', 'SECM-S0002_00030'], ['SOUKAIMY', 'SECM-S0002_00032'],
                    ['SOUKAIMY', 'SECM-S0002_00105']]
        self.assertEqual(5, len(result[1:]))
        self.assertListEqual(expected, result[1:])

    def test_retrieve_tenants_10_15(self):
        main_task = TaskRetrieveTenantsItemsListMocked(date='2015-09-01', tenant='SOUKAIMY', start=10, end=15, id=1)
        luigi.build([main_task], local_scheduler=True)

        # Check the result
        bio = io.StringIO(MockTarget.fs.get_data('/tmp/download.json').decode('utf-8'))
        result = list(csv.reader(bio))
        expected = [['SOUKAIMY', 'SECM-S0012_00269'], ['SOUKAIMY', 'SECM-S0014_00004'],
                    ['SOUKAIMY', 'SECM-S0014_00005'], ['SOUKAIMY', 'SECM-S0017_00011'],
                    ['SOUKAIMY', 'SECM-S0018_00051']]
        self.assertEqual(5, len(result[1:]))
        self.assertListEqual(expected, result[1:])

    def test_retrieve_tenants_0_10_3(self):
        main_task = TaskRetrieveTenantsItemsListMocked(date='2015-09-01', tenant='SOUKAIMY', start=0, end=10,
                                                       limit=3, id=1)
        luigi.build([main_task], local_scheduler=True)

        # Check the result
        bio = io.StringIO(MockTarget.fs.get_data('/tmp/download.json').decode('utf-8'))
        result = list(csv.reader(bio))
        expected = [['SOUKAIMY', 'SECM-S0001_00322'], ['SOUKAIMY', 'SECM-S0001_00706'],
                    ['SOUKAIMY', 'SECM-S0002_00030'], ['SOUKAIMY', 'SECM-S0002_00032'],
                    ['SOUKAIMY', 'SECM-S0002_00105'], ['SOUKAIMY', 'SECM-S0005_00119'],
                    ['SOUKAIMY', 'SECM-S0007_00019'], ['SOUKAIMY', 'SECM-S0008_00014'],
                    ['SOUKAIMY', 'SECM-S0012_00025'], ['SOUKAIMY', 'SECM-S0012_00137']]
        self.assertEqual(10, len(result[1:]))
        self.assertListEqual(expected, result[1:])

    def test_retrieve_tenants_10_15_3(self):
        main_task = TaskRetrieveTenantsItemsListMocked(date='2015-09-01', tenant='SOUKAIMY', start=10, end=15, id=1)
        luigi.build([main_task], local_scheduler=True)

        # Check the result
        bio = io.StringIO(MockTarget.fs.get_data('/tmp/download.json').decode('utf-8'))
        result = list(csv.reader(bio))
        expected = [['SOUKAIMY', 'SECM-S0012_00269'], ['SOUKAIMY', 'SECM-S0014_00004'],
                    ['SOUKAIMY', 'SECM-S0014_00005'], ['SOUKAIMY', 'SECM-S0017_00011'],
                    ['SOUKAIMY', 'SECM-S0018_00051']]
        self.assertEqual(5, len(result[1:]))
        self.assertListEqual(expected, result[1:])

    def test_compute_recommendation(self):
        main_task = TaskComputeRecommendationsMocked(date='2015-09-01', tenant='SOUKAIMY', start=0, end=10, id=1,
                                                     algorithm='duo')
        luigi.build([main_task], local_scheduler=True)

        # Check the result
        sio = io.StringIO(MockTarget.fs.get_data('/tmp/compute.json').decode('utf-8'))
        result = list(csv.reader(sio))
        expected = [["SOUKAIMY", "SECM-S0001_00322", "1", "duo", ["SECM-S0001_00706"]],
                    ["SOUKAIMY", "SECM-S0001_00706", "1", "duo", ["SECM-S0001_00322"]],
                    ["SOUKAIMY", "SECM-S0002_00030", "2", "duo", ["SECM-S0002_00032", "SECM-S0002_00105"]],
                    ["SOUKAIMY", "SECM-S0002_00032", "2", "duo", ["SECM-S0002_00105", "SECM-S0002_00030"]],
                    ["SOUKAIMY", "SECM-S0002_00105", "2", "duo", ["SECM-S0002_00032", "SECM-S0002_00030"]],
                    ["SOUKAIMY", "SECM-S0005_00119", "0", "duo", []],
                    ["SOUKAIMY", "SECM-S0007_00019", "0", "duo", []],
                    ["SOUKAIMY", "SECM-S0008_00014", "0", "duo", []],
                    ["SOUKAIMY", "SECM-S0012_00025", "0", "duo", []],
                    ["SOUKAIMY", "SECM-S0012_00137", "0", "duo", []]]
        self.assertEqual(10, len(result[1:]))
        for expectedLine, resultLine in zip(expected, result[1:]):
            self.assertEqual(resultLine[0], expectedLine[0])
            self.assertEqual(resultLine[1], expectedLine[1])
            self.assertEqual(resultLine[2], expectedLine[2])
            self.assertEqual(resultLine[3], expectedLine[3])
            for code in expectedLine[4]:
                self.assertTrue(code in resultLine[4])

    @patch('neumann.core.aws.S3')
    def test_store_recommendation(self, s3):
        config.config.set('output', 'targettenants', 'SOUKAIMY')
        main_task = TaskStoreRecommendationResultsMocked(date='2015-09-01', tenant='SOUKAIMY', start=0, end=10, id=1,
                                                         algorithm='duo')
        luigi.build([main_task], local_scheduler=True)

        # S3 is mocked to make sure it is not performing actual S3 operations, but check if it is called
        self.assertTrue(s3.sync.called)
        self.assertEqual(1, s3.sync.call_count)

        # Check the result
        sio = io.StringIO(MockTarget.fs.get_data('/tmp/store.json').decode('utf-8'))
        result = list(csv.reader(sio))
        expected = [
            ["SOUKAIMY", "SECM-S0001_00322", "SECM-S0001_00322.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0001_00322.json"],
            ["SOUKAIMY", "SECM-S0001_00706", "SECM-S0001_00706.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0001_00706.json"],
            ["SOUKAIMY", "SECM-S0002_00030", "SECM-S0002_00030.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0002_00030.json"],
            ["SOUKAIMY", "SECM-S0002_00032", "SECM-S0002_00032.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0002_00032.json"],
            ["SOUKAIMY", "SECM-S0002_00105", "SECM-S0002_00105.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0002_00105.json"],
            ["SOUKAIMY", "SECM-S0005_00119", "SECM-S0005_00119.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0005_00119.json"],
            ["SOUKAIMY", "SECM-S0007_00019", "SECM-S0007_00019.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0007_00019.json"],
            ["SOUKAIMY", "SECM-S0008_00014", "SECM-S0008_00014.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0008_00014.json"],
            ["SOUKAIMY", "SECM-S0012_00025", "SECM-S0012_00025.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0012_00025.json"],
            ["SOUKAIMY", "SECM-S0012_00137", "SECM-S0012_00137.json",
             "/var/neumann/data/2015-09-01/TaskStoreRecommendationResultsMocked/SOUKAIMY/duo/1/SECM-S0012_00137.json"]]
        self.assertEqual(10, len(result[1:]))
        for expectedLine, resultLine in zip(expected, result[1:]):
            self.assertListEqual(expectedLine, resultLine)
            # Make sure the file is deleted after the task was done
            self.assertFalse(os.path.exists(expectedLine[3]))

        # Check if generated json files are copied properly to s3copy folder
        s3copy_dir = os.path.join('/var/neumann', config.get('output', 'dir'), config.get("s3")["folder"], 'SOUKAIMY',
                                  'recommendations', 'duo')
        for expectedLine in expected:
            self.assertTrue(os.path.exists(os.path.join(s3copy_dir, expectedLine[2])))

    # noinspection PyUnresolvedReferences
    @patch('neumann.workflows.TaskStoreRecommendationResults')
    @patch.object(neumann.core.repository.Neo4jRepository, 'get_item_count_for_tenant')
    def test_run_recommendation_10(self, repo_get_item_count, mocked_dependencies):
        repo_get_item_count.return_value = 10
        main_task = neumann.workflows.TaskRunRecommendationWorkflow(date='2015-09-01', tenant='SOUKAIMY',
                                                                    algorithm='duo', job_size=10)
        main_task.run = lambda: None
        luigi.build([main_task], local_scheduler=True)

        # Check the task distribution (in required tasks section)
        self.assertEqual(2, repo_get_item_count.call_count)
        self.assertEqual(2, mocked_dependencies.call_count)
        mocked_dependencies.assert_any_call(id=1, start=0, end=10, date='2015-09-01', tenant='SOUKAIMY',
                                            algorithm='duo')

    # noinspection PyUnresolvedReferences
    @patch('neumann.workflows.TaskStoreRecommendationResults')
    @patch.object(neumann.core.repository.Neo4jRepository, 'get_item_count_for_tenant')
    def test_run_recommendation_20(self, repo_get_item_count, mocked_dependencies):
        repo_get_item_count.return_value = 20
        main_task = neumann.workflows.TaskRunRecommendationWorkflow(date='2015-09-01', tenant='SOUKAIMY',
                                                                    algorithm='duo', job_size=10)
        main_task.run = lambda: None
        luigi.build([main_task], local_scheduler=True)

        # Check the task distribution (in required tasks section)
        self.assertEqual(2, repo_get_item_count.call_count)
        self.assertEqual(4, mocked_dependencies.call_count)
        mocked_dependencies.assert_any_call(id=1, start=0, end=10, date='2015-09-01', tenant='SOUKAIMY',
                                            algorithm='duo')
        mocked_dependencies.assert_any_call(id=2, start=10, end=20, date='2015-09-01', tenant='SOUKAIMY',
                                            algorithm='duo')

    # noinspection PyUnresolvedReferences
    @patch('neumann.workflows.TaskStoreRecommendationResults')
    @patch.object(neumann.core.repository.Neo4jRepository, 'get_item_count_for_tenant')
    def test_run_recommendation_23(self, repo_get_item_count, mocked_dependencies):
        repo_get_item_count.return_value = 23
        main_task = neumann.workflows.TaskRunRecommendationWorkflow(date='2015-09-01', tenant='SOUKAIMY',
                                                                    algorithm='duo', job_size=10)
        main_task.run = lambda: None
        luigi.build([main_task], local_scheduler=True)

        # Check the task distribution (in required tasks section)
        self.assertEqual(2, repo_get_item_count.call_count)
        self.assertEqual(6, mocked_dependencies.call_count)
        mocked_dependencies.assert_any_call(id=1, start=0, end=10, date='2015-09-01', tenant='SOUKAIMY',
                                            algorithm='duo')
        mocked_dependencies.assert_any_call(id=2, start=10, end=20, date='2015-09-01', tenant='SOUKAIMY',
                                            algorithm='duo')
        mocked_dependencies.assert_any_call(id=3, start=20, end=23, date='2015-09-01', tenant='SOUKAIMY',
                                            algorithm='duo')
