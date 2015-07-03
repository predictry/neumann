import sys
import subprocess
import os.path

from redis import Redis
from rq.decorators import job
from neumann.workflows import harvestwk
from neumann.workflows import recommendwk
from neumann.utils.logger import Logger
from neumann.core import errors
from neumann.utils import config
from neumann.ops.interfaces import ITask

_redis_conn = Redis()

TASK_STATUS_WAITING = 'WAITING'
TASK_STATUS_RUNNING = 'RUNNING'
TASK_STATUS_STOPPED = 'STOPPED'

TASK_TYPE_COMPUTEREC = 'COMPUTEREC'
TASK_TYPE_HARVESTDATA = 'HARVESTDATA'

TASK_TYPES = (
    TASK_TYPE_HARVESTDATA,
    TASK_TYPE_COMPUTEREC
)

class RecordImportTask(ITask):

    def __init__(self, timestamp, tenant):
        self.timestamp = timestamp
        self.tenant = tenant

    @job('low', connection=_redis_conn, timeout=int(config.get('harvester', 'timeout')))
    def run(self):

        filepath = os.path.abspath(harvestwk.__file__)
        classname = harvestwk.TaskImportRecordIntoNeo4j.__name__

        Logger.info([sys.executable, filepath, classname,
                     '--date', str(self.timestamp.date()),
                     '--hour', str(self.timestamp.hour),
                     '--tenant', self.tenant])

        p = subprocess.Popen([sys.executable, filepath, classname,
                              '--date', str(self.timestamp.date()),
                              '--hour', str(self.timestamp.hour),
                              '--tenant', self.tenant])

        stdout, stderr = p.communicate()

        if stderr:

            Logger.error(stderr)

            message = '{0} ({1}, {2}) failed'.format(
                classname, str(self.timestamp.date()), self.timestamp.hour
            )

            raise errors.ProcessFailureError(
                message
            )

        else:

            Logger.info(stdout)

            if p.returncode == 0:
                return True
            else:

                message = '{0} ({1}, {2}, {3}) failed'.format(
                    classname, str(self.timestamp.date()), self.timestamp.hour, self.tenant
                )

                raise errors.ProcessFailureError(
                    message
                )


class ComputeRecommendationTask(ITask):

    def __init__(self, date, tenant):
        self.date = date
        self.tenant = tenant

    @job('high', connection=_redis_conn, timeout=3600*2)
    def run(self):

        filepath = os.path.abspath(recommendwk.__file__)
        classname = recommendwk.TaskRunRecommendationWorkflow.__name__

        Logger.info([sys.executable, filepath, classname,
                     '--date', str(self.date),
                     '--tenant', self.tenant])

        p = subprocess.Popen([sys.executable, filepath, classname,
                              '--date', str(self.date),
                              '--tenant', self.tenant])

        stdout, stderr = p.communicate()

        if stderr:

            Logger.error(stderr)

            message = '{0} ({1}, {2}) failed'.format(
                classname, str(self.date), self.tenant
            )

            raise errors.ProcessFailureError(
                message
            )

        else:

            Logger.info(stdout)

            if p.returncode == 0:
                return True
            else:

                message = '{0} ({1}, {2}) failed'.format(
                    classname, str(self.date), self.tenant
                )

                raise errors.ProcessFailureError(
                    message
                )
