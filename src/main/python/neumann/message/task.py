from datetime import datetime, timedelta
from abc import abstractmethod, ABCMeta
from neumann.message.datasource import S3DataSource, HttpDataSource, TapirusDataSource
from neumann import services


class Task(metaclass=ABCMeta):
    """
    ``Task`` is a parent abstract class for all available tasks.
    """

    @abstractmethod
    def required_fields(self):
        """:rtype: list[str]"""
        pass

    @abstractmethod
    def execute(self):
        pass

    def available_datasources(self):
        return {
            's3': S3DataSource,
            'http': HttpDataSource,
            'tapirus': TapirusDataSource
        }

    def parse(self, payload):
        for field in self.required_fields():
            if 'payload' not in payload:
                raise ValueError("Can't find 'payload' in: {0}".format(payload))
            if field not in payload['payload']:
                raise ValueError("Can't find '{0}' in data source: {1}".format(field, payload))
        self.payload = payload

    def can_parse(self, payload):
        return False

    def __getattr__(self, attr):
        if attr == 'type':
            return self.payload['type']
        elif attr in self.payload['payload']:
            return self.payload['payload'][attr]
        else:
            raise AttributeError

    def get_payload(self, attr):
        return self.payload['payload'][attr]


class TrimDataTask(Task):

    def required_fields(self):
        return ['tenant', 'date', 'startingDate', 'period']

    def can_parse(self, payload):
        return payload['type'] == 'trim-data'

    def execute(self, job_id='default_job_id'):
        services.DataTrimmingService.trim(date=self.get_payload('date'), tenant=self.get_payload('tenant'),
                                          starting_date=self.get_payload('startingDate'),
                                          period=self.get_payload('period'), job_id=job_id)


class ComputeRecommendationTask(Task):

    def required_fields(self):
        return ['tenant', 'date', 'algorithm', 'output']

    def can_parse(self, payload):
        return payload['type'] == 'compute-recommendation'

    def parse(self, payload):
        super().parse(payload)
        datasource = self.available_datasources()[payload['payload']['output']['type']]()
        if datasource.can_parse(payload['payload']['output']):
            self.output = datasource
            self.output.parse(payload['payload']['output'])

    def execute(self, job_id='default_job_id'):
        services.RecommendService.compute(date=self.get_payload('date'), tenant=self.get_payload('tenant'),
                                          algorithm=self.get_payload('algorithm')['name'], job_id=job_id)


class ImportRecordTask(Task):

    def required_fields(self):
        return ['tenant', 'date', 'hour', 'input']

    def can_parse(self, payload):
        return payload['type'] == 'import-record'

    def parse(self, payload):
        super().parse(payload)
        datasource = self.available_datasources()[payload['payload']['input']['type']]()
        if datasource.can_parse(payload['payload']['input']):
            self.input = datasource
            self.input.parse(payload['payload']['input'])

    def execute(self, job_id='default_job_id'):
        timestamp = datetime.strptime(self.get_payload('date'), '%Y-%m-%d') + timedelta(hours=self.get_payload('hour'))
        services.RecordImportService.harvest(timestamp=timestamp, tenant=self.get_payload('tenant'), job_id=job_id)


class SyncItemStoreTask(Task):

    def required_fields(self):
        return ['tenant', 'target']

    def can_parse(self, payload):
        return payload['type'] == 'sync-item-store'

    def parse(self, payload):
        super().parse(payload)
        datasource = self.available_datasources()[payload['payload']['target']['type']]()
        if datasource.can_parse(payload['payload']['target']):
            self.target = datasource
            self.target.parse(payload['payload']['target'])

    def execute(self, job_id='default_job_id'):
        # Do we still need this?
        pass
