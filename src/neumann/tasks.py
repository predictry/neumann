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

_redis_conn = Redis()


@job('medium', connection=_redis_conn, timeout=int(config.get('harvester', 'timeout')))
def run_record_import_task(timestamp, tenant):

    assert isinstance(timestamp, datetime.datetime)

    filepath = os.path.abspath(harvestwk.__file__)
    classname = harvestwk.TaskImportRecordIntoNeo4j.__name__

    Logger.info([sys.executable, filepath, classname,
                 '--date', str(timestamp.date()),
                 '--hour', str(timestamp.hour),
                 '--tenant', tenant])

    p = subprocess.Popen([sys.executable, filepath, classname,
                          '--date', str(timestamp.date()),
                          '--hour', str(timestamp.hour),
                          '--tenant', tenant])

    stdout, stderr = p.communicate()

    if stderr:

        Logger.error(stderr)

        message = '{0} ({1}, {2}) failed'.format(
            classname, str(timestamp.date()), timestamp.hour
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
                classname, str(timestamp.date()), timestamp.hour
            )

            raise errors.ProcessFailureError(
                message
            )

# TODO: add task for computing recommendations, with higher priority (high)