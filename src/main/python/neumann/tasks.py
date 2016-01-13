import sys
import subprocess
import os.path
import configparser
import ujson

from redis import Redis
from rq.decorators import job

from neumann import workflows
from neumann.utils.logger import Logger
from neumann.core import errors
from neumann.utils.config import TASK_CONFIG_FILE
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


def taskconfig(task, option=None, fallback=None, type=None):

    config = configparser.ConfigParser()

    with open(TASK_CONFIG_FILE, "r") as fp:
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

    def __init__(self, timestamp, tenant, job_id='default_job_id'):
        self.timestamp = timestamp
        self.tenant = tenant
        self.job_id = job_id

    def __str__(self):

        return '{0}[timestamp={1}, tenant={2}, job_id={3}]'.format(
            self.__class__.__name__, str(self.timestamp), self.tenant, self.job_id
        )

    def run(self):

        _import_record_task.delay(timestamp=self.timestamp, tenant=self.tenant, job_id=self.job_id)


@job('default', connection=_redis_conn, timeout=int(taskconfig('import-record', 'timeout', 1800)))
def _import_record_task(timestamp, tenant, job_id='default_job_id'):

    def execute():

        filepath = os.path.abspath(workflows.__file__)
        classname = workflows.TaskImportRecordIntoNeo4j.__name__
        workers = taskconfig('import-record', 'workers', 1)

        statements = [sys.executable, filepath, classname,
                      '--job-id', job_id,
                      '--date', str(timestamp.date()),
                      '--hour', str(timestamp.hour),
                      '--tenant', tenant,
                      '--workers', str(workers)]

        Logger.info(
            '\t'.join(statements)
        )

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

    return execute()


class ComputeRecommendationTask(ITask):

    def __init__(self, date, tenant, algorithm, job_id='default_job_id', whitelist=None, blacklist=None):
        self.date = date
        self.tenant = tenant
        self.algorithm = algorithm
        self.job_id = job_id
        self.whitelist = whitelist
        self.blacklist = blacklist

    def run(self):
        _compute_recommendation_task.delay(date=self.date, tenant=self.tenant, algorithm=self.algorithm,
                                           job_id=self.job_id, whitelist=self.whitelist, blacklist=self.blacklist)

    def __str__(self):

        return '{0}[date={1}, tenant={2}, job_id={3}]'.format(
            self.__class__.__name__, self.date, self.tenant, self.job_id
        )


@job('default', connection=_redis_conn, timeout=int(taskconfig('recommend', 'timeout', 3600*2)))
def _compute_recommendation_task(date, tenant, algorithm, job_id='default_job_id', whitelist=None, blacklist=None):
    
    def execute():

        filepath = os.path.abspath(workflows.__file__)
        classname = workflows.TaskRunRecommendationWorkflow.__name__
        workers = taskconfig('recommend', 'workers', 4)

        statements = [sys.executable, filepath, classname,
                      '--job-id', job_id,
                      '--date', str(date),
                      '--tenant', tenant,
                      '--algorithm', algorithm,
                      '--workers', str(workers)]

        if whitelist:
            statements.append('--whitelist')
            statements.append(ujson.dumps(whitelist))

        if blacklist:
            statements.append('--blacklist')
            statements.append(ujson.dumps(blacklist))

        Logger.info(
            '\t'.join(statements)
        )

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


class TrimDataTask(ITask):

    def __init__(self, date, tenant, starting_date, period, job_id='default_job_id'):
        self.date = date
        self.tenant = tenant
        self.starting_date = starting_date
        self.period = period
        self.job_id = job_id

    def run(self):

        _trim_data_task.delay(date=self.date, tenant=self.tenant, starting_date=self.starting_date,
                              period=self.period, job_id=self.job_id)

    def __str__(self):

        return '{0}[date={1}, tenant={2}, startingDate={3}, period={4}, job_id={5}]'.format(
            self.__class__.__name__, self.date, self.tenant,
            self.starting_date, self.period, self.job_id
        )


@job('default', connection=_redis_conn, timeout=3600)
def _trim_data_task(date, tenant, starting_date, period, job_id='default_job_id'):

    def execute():

        filepath = os.path.abspath(workflows.__file__)
        classname = workflows.TaskRunTrimDataWorkflow.__name__
        workers = taskconfig('trim-data', 'workers', 1)

        statements = [sys.executable, filepath, classname,
                      '--job-id', job_id,
                      '--date', str(date),
                      '--tenant', tenant,
                      '--starting-date', str(starting_date),
                      '--period', str(period),
                      '--workers', str(workers)]

        Logger.info(
            '\t'.join(statements)
        )

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
