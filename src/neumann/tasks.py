import sys
import subprocess
import os.path
import configparser

from redis import Redis
from rq.decorators import job

from neumann import workflows
from neumann.utils.logger import Logger
from neumann.core import errors
from neumann.utils import config
from neumann.ops.interfaces import ITask

_redis_conn = Redis()

TASK_STATUS_WAITING = 'WAITING'
TASK_STATUS_RUNNING = 'RUNNING'
TASK_STATUS_STOPPED = 'STOPPED'

TASK_TYPE_COMPUTEREC = 'recommend'
TASK_TYPE_RECORDIMPORT = 'import-record'
TASK_TYPE_TRIMDATA = 'trim-data'

TASK_TYPES = (
    TASK_TYPE_RECORDIMPORT,
    TASK_TYPE_COMPUTEREC,
    TASK_TYPE_TRIMDATA
)

TASK_CONFIG = os.path.join(config.PROJECT_BASE, 'tasks.ini')


def taskconfig(task, option=None, fallback=None, type=None):

    config = configparser.ConfigParser()

    with open(TASK_CONFIG, "r") as fp:
        config.read_file(fp)

        if option:

            try:
                value = config.get(task, option, fallback=fallback)
            except configparser.NoOptionError as exc:
                raise errors.ConfigurationError(exc)
            else:

                if type and hasattr(type, '__call__'):
                    return type(value)
                else:
                    return value
        else:

            try:
                data = dict(config.items(task))
            except configparser.NoSectionError as exc:
                raise errors.ConfigurationError(exc)
            else:
                return data


class ImportRecordTask(ITask):

    def __init__(self, timestamp, tenant):
        self.timestamp = timestamp
        self.tenant = tenant

    def __str__(self):

        return '{0}[timestamp={1}, tenant={2}]'.format(
            self.__class__.__name__, str(self.timestamp), self.tenant
        )

    def run(self):

        _import_record_task.delay(self.timestamp, self.tenant)


@job('low', connection=_redis_conn, timeout=int(taskconfig('import-record', 'timeout', 1800)))
def _import_record_task(timestamp, tenant):

    def execute(timestamp, tenant):

        filepath = os.path.abspath(workflows.__file__)
        classname = workflows.TaskImportRecordIntoNeo4j.__name__
        workers = taskconfig('import-record', 'workers', 1)

        statements = [sys.executable, filepath, classname,
                      '--date', str(timestamp.date()),
                      '--hour', str(timestamp.hour),
                      '--tenant', tenant,
                      '--workers', str(workers)]

        Logger.info(statements)

        p = subprocess.Popen(statements)

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

                message = '{0} ({1}, {2}, {3}) failed'.format(
                    classname, str(timestamp.date()), timestamp.hour, tenant
                )

                raise errors.ProcessFailureError(
                    message
                )

    return execute(timestamp=timestamp, tenant=tenant)


class ComputeRecommendationTask(ITask):

    def __init__(self, date, tenant):
        self.date = date
        self.tenant = tenant

    def run(self):
        _compute_recommendation_task.delay(self.date, self.tenant)

    def __str__(self):

        return '{0}[date={1}, tenant={2}]'.format(
            self.__class__.__name__, self.date, self.tenant
        )


@job('high', connection=_redis_conn, timeout=int(taskconfig('recommend', 'timeout', 3600*2)))
def _compute_recommendation_task(date, tenant):
    
    def execute(date, tenant):

        filepath = os.path.abspath(workflows.__file__)
        classname = workflows.TaskRunRecommendationWorkflow.__name__
        workers = taskconfig('recommend', 'workers', 4)

        statements = [sys.executable, filepath, classname,
                      '--date', str(date),
                      '--tenant', tenant,
                      '--workers', str(workers)]

        Logger.info(statements)

        p = subprocess.Popen(statements)

        stdout, stderr = p.communicate()

        if stderr:

            Logger.error(stderr)

            message = '{0} ({1}, {2}) failed'.format(
                classname, str(date), tenant
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
                    classname, str(date), tenant
                )

                raise errors.ProcessFailureError(
                    message
                )
    
    return execute(date=date, tenant=tenant)


class TrimDataTask(ITask):

    def __init__(self, date, tenant, starting_date, period):
        self.date = date
        self.tenant = tenant
        self.starting_date = starting_date
        self.period = period

    def run(self):

        _trim_data_task.delay(self.date, self.tenant, self.starting_date, self.period)

    def __str__(self):

        return '{0}[date={1}, tenant={2}, period={3}]'.format(
            self.__class__.__name__, self.date, self.tenant, self.period
        )


@job('high', connection=_redis_conn, timeout=3600)
def _trim_data_task(date, tenant, starting_date, period):

    def execute():

        filepath = os.path.abspath(workflows.__file__)
        classname = workflows.TaskRunTrimDataWorkflow.__name__
        workers = taskconfig('trim-data', 'workers', 1)

        statements = [sys.executable, filepath, classname,
                      '--date', str(date),
                      '--tenant', tenant,
                      '--starting_date', str(starting_date),
                      '--period', str(period),
                      '--workers', str(workers)]

        Logger.info(statements)

        p = subprocess.Popen(statements)

        stdout, stderr = p.communicate()

        if stderr:

            Logger.error(stderr)

            message = '{0} ({1}, {2}) failed'.format(
                classname, str(date), tenant
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
                    classname, str(date), tenant
                )

                raise errors.ProcessFailureError(
                    message
                )

    return execute()
