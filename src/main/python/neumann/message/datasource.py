from abc import abstractmethod, ABCMeta


class DataSource(metaclass=ABCMeta):
    """
    ``DataSource`` is a parent abstract class for all available data sources.
    """

    @abstractmethod
    def required_fields(self):
        """:rtype: list[str]"""
        pass

    def parse(self, payload):
        for field in self.required_fields():
            if field not in payload:
                raise ValueError("Can't find '{0}' in data source: {1}".format(field, payload))
        self.payload = payload

    def get_payload(self):
        return self.payload

    @classmethod
    def can_parse(cls, payload):
        return False

    def __getattr__(self, attr):
        if attr in self.payload:
            return self.payload[attr]
        else:
            raise AttributeError


class S3DataSource(DataSource):

    def required_fields(self):
        return ['bucket', 'prefix', 'region']

    @classmethod
    def can_parse(cls, payload):
        return payload['type'] == 's3'


class HttpDataSource(DataSource):

    def required_fields(self):
        return ['host', 'port', 'path']

    @classmethod
    def can_parse(cls, payload):
        return payload['type'] == 'http'


class TapirusDataSource(DataSource):

    def required_fields(self):
        return ['host', 'port']

    @classmethod
    def can_parse(cls, payload):
        return payload['type'] == 'tapirus'
