import stomp
import ujson
from neumann.utils.logger import Logger
from neumann.message.task import TrimDataTask, ComputeRecommendationTask, ImportRecordTask, SyncItemStoreTask


class CommandMessage:

    def __init__(self, message):
        self.json = ujson.loads(message)

        # Check structure of the Json message
        if 'jobId' not in self.json:
            raise ValueError('Invalid Json command. No "jobId" found in: {0}'.format(message))
        if 'type' not in self.json:
            raise ValueError('Invalid Json command. No "type" found in: {0}'.format(message))
        if 'payload' not in self.json:
            raise ValueError('Invalid Json command. No "payload" found in: {0}'.format(message))

        # Assign task
        available_tasks = {
            'trim-data': TrimDataTask(),
            'compute-recommendation': ComputeRecommendationTask(),
            'import-record': ImportRecordTask(),
            'sync-item-store': SyncItemStoreTask()
        }
        if self.json['type'] in available_tasks.keys():
            self.task = available_tasks[self.json['type']]
        else:
            raise ValueError('Invalid type for command message: {0}'.format(self.json['type']))

        self.task.parse(self.json)
        self.job_id = self.json['jobId']

    def get_type(self):
        return self.json['type']

    def execute(self):
        self.task.execute(self.job_id)


class CommandEventListener(stomp.ConnectionListener):

    def on_error(self, headers, message):
        Logger.error('Received error for message [{0}] headers [{1}]'.format(message, headers))

    def on_message(self, headers, message):
        Logger.info('Received message [{0}] headers [{1}]'.format(message, headers))
        try:
            command = CommandMessage(message)
            command.execute()
        except Exception as e:
            Logger.error('Unexpection error: {0}'.format(e))
            Logger.exception(e)
