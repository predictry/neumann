import unittest
from neumann.message.datasource import S3DataSource, HttpDataSource, TapirusDataSource


class MessageTest(unittest.TestCase):

    def test_s3_datasource(self):
        s3_datasource = S3DataSource()
        valid_payload = {
            'type': 's3',
            'bucket': 'exercises',
            'prefix': "photos/2006/January/sample.jpg",
            'region': 'eu-west-1'
        }
        invalid_payload = {
            'type': 'tapirus',
            'user': 'unknown'
        }
        self.assertTrue(s3_datasource.can_parse(valid_payload))
        self.assertFalse(s3_datasource.can_parse(invalid_payload))
        s3_datasource.parse(valid_payload)
        with self.assertRaises(ValueError):
            s3_datasource.parse(invalid_payload)

    def test_http_datasource(self):
        http_datasource = HttpDataSource()
        valid_payload = {
            "type": "http",
            "host": "www.xxx.com",
            "port": 8080,
            "path": "/January/data"
        }
        invalid_payload = {
            "type": "s3",
            "user": "unknown"
        }
        self.assertTrue(http_datasource.can_parse(valid_payload))
        self.assertFalse(http_datasource.can_parse(invalid_payload))
        http_datasource.parse(valid_payload)
        with self.assertRaises(ValueError):
            http_datasource.parse(invalid_payload)

    def test_tapirus_datasource(self):
        tapirus_datasource = TapirusDataSource()
        valid_payload = {
            "type": "tapirus",
            "host": "192.168.0.100",
            "port": 1999
        }
        invalid_payload = {
            "type": "s3",
            "user": "unknown"
        }
        self.assertTrue(tapirus_datasource.can_parse(valid_payload))
        self.assertFalse(tapirus_datasource.can_parse(invalid_payload))
        tapirus_datasource.parse(valid_payload)
        with self.assertRaises(ValueError):
            tapirus_datasource.parse(invalid_payload)
