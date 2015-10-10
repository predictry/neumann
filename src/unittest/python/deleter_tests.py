import unittest
import os
import shutil
import json
import neumann.tools.deleter
from neumann.utils import config


class DeleterTest(unittest.TestCase):

    def setUp(self):
        config.config.set('output', 'targettenants', 'TENANT1')
        config.config.set('output', 'dir', 'test')
        if os.path.exists('/var/neumann/test'):
            shutil.rmtree('/var/neumann/test')
        os.makedirs('/var/neumann/test/algo1')
        os.makedirs('/var/neumann/test/algo2')
        with open('/var/neumann/test/algo1/product1.json', 'w') as file1:
            json.dump({'algo': 'algo1', 'items': ['product2', 'product3']}, file1)
        with open('/var/neumann/test/algo1/product2.json', 'w') as file2:
            json.dump({'algo': 'algo1', 'items': ['product1', 'product2']}, file2)
        with open('/var/neumann/test/algo1/product3.json', 'w') as file3:
            json.dump({}, file3)
        with open('/var/neumann/test/algo2/product1.json', 'w') as file4:
            json.dump({'algo': 'algo2', 'items': ['product2']}, file4)
        with open('/var/neumann/test/algo2/product2.json', 'w') as file5:
            json.dump({'algo': 'algo2', 'items': ['product2', 'product1']}, file5)

    def test_delete_1(self):
        listener = neumann.tools.deleter.DeleteEventListener()
        listener.on_message(None, json.dumps({'tenantId': 'TENANT1', 'id': 'product1'}))
        self.assertTrue(not os.path.exists('/var/neumann/test/algo1/product1.json'))
        with open('/var/neumann/test/algo1/product2.json', 'r') as file:
            data = json.load(file)
            self.assertEqual('algo1', data['algo'])
            self.assertCountEqual(['product2'], data['items'])
        self.assertTrue(os.path.exists('/var/neumann/test/algo1/product3.json'))
        self.assertTrue(not os.path.exists('/var/neumann/test/algo2/product1.json'))
        with open('/var/neumann/test/algo2/product2.json', 'r') as file:
            data = json.load(file)
            self.assertEqual('algo2', data['algo'])
            self.assertCountEqual(['product2'], data['items'])

    def test_delete_2(self):
        listener = neumann.tools.deleter.DeleteEventListener()
        listener.on_message(None, json.dumps({'tenantId': 'TENANT1', 'id': 'product2'}))
        with open('/var/neumann/test/algo1/product1.json', 'r') as file:
            data = json.load(file)
            self.assertEqual('algo1', data['algo'])
            self.assertCountEqual(['product3'], data['items'])
        self.assertTrue(not os.path.exists('/var/neumann/test/algo1/product2.json'))
        self.assertTrue(os.path.exists('/var/neumann/test/algo1/product3.json'))
        with open('/var/neumann/test/algo2/product1.json', 'r') as file:
            data = json.load(file)
            self.assertEqual('algo2', data['algo'])
            self.assertEqual(0, len(data['items']))
        self.assertTrue(not os.path.exists('/var/neumann/test/algo2/product2.json'))

    def test_delete_3(self):
        listener = neumann.tools.deleter.DeleteEventListener()
        listener.on_message(None, json.dumps({'tenantId': 'TENANT1', 'id': 'product3'}))
        with open('/var/neumann/test/algo1/product1.json', 'r') as file:
            data = json.load(file)
            self.assertEqual('algo1', data['algo'])
            self.assertCountEqual(['product2'], data['items'])
        with open('/var/neumann/test/algo1/product2.json', 'r') as file:
            data = json.load(file)
            self.assertEqual('algo1', data['algo'])
            self.assertCountEqual(['product1', 'product2'], data['items'])
        self.assertTrue(not os.path.exists('/var/neumann/test/algo1/product3.json'))
        with open('/var/neumann/test/algo2/product1.json', 'r') as file:
            data = json.load(file)
            self.assertEqual('algo2', data['algo'])
            self.assertCountEqual(['product2'], data['items'])
        with open('/var/neumann/test/algo2/product2.json', 'r') as file:
            data = json.load(file)
            self.assertEqual('algo2', data['algo'])
            self.assertCountEqual(['product2', 'product1'], data['items'])
