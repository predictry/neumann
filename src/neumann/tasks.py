import datetime
import sys
import subprocess
import os.path

from redis import Redis
from rq.decorators import job

from neumann.workflows import harvestwk
from neumann.utils.logger import Logger
from neumann.core import errors
from neumann.utils import config
from neumann.ops import ITask

_redis_conn = Redis()

@job('low', connection=_redis_conn, timeout=int(config.get('harvester', 'timeout')))
class RecordImportTask(ITask):

    def __init__(self, tenant, timestamp):

        assert isinstance(self.timestamp, datetime.datetime)

        self.tenant = tenant
        self.timestamp = timestamp

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

                message = '{0} ({1}, {2}) failed'.format(
                    classname, str(self.timestamp.date()), self.timestamp.hour
                )

                raise errors.ProcessFailureError(
                    message
                )

# TODO: add task for computing recommendations, with higher priority (high)