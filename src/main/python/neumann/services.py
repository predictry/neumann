from neumann import tasks
from neumann.tasks import ImportRecordTask
from neumann.tasks import ComputeRecommendationTask
from neumann.tasks import TrimDataTask
from neumann.utils.logger import Logger


class RecordImportService(object):

    @classmethod
    def harvest(cls, timestamp, tenant, job_id='default_job_id'):

        job = ImportRecordTask(timestamp=timestamp, tenant=tenant, job_id=job_id)

        # queue task
        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_RECORDIMPORT,
                    parameters=dict(date=str(timestamp.date()), hour=timestamp.hour, tenant=tenant))


class RecommendService(object):

    @classmethod
    def compute(cls, date, tenant, algorithm, job_id='default_job_id', whitelist=None, blacklist=None):

        job = ComputeRecommendationTask(date=date, tenant=tenant, algorithm=algorithm, job_id=job_id,
                                        whitelist=whitelist, blacklist=blacklist)

        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_COMPUTEREC, parameters=dict(date=date, tenant=tenant, algorithm=algorithm))


class DataTrimmingService(object):

    @classmethod
    def trim(cls, date, tenant, starting_date, period, job_id='default_job_id'):

        job = TrimDataTask(date=date, tenant=tenant, starting_date=starting_date, period=period, job_id=job_id)

        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_TRIMDATA, parameters=dict(date=date, tenant=tenant, period=period))
