from neumann import tasks
from neumann.tasks import ImportRecordTask
from neumann.tasks import ComputeRecommendationTask
from neumann.tasks import TrimDataTask
from neumann.utils.logger import Logger


class RecordImportService(object):

    @classmethod
    def harvest(cls, timestamp, tenant):

        job = ImportRecordTask(timestamp=timestamp, tenant=tenant)

        # queue task
        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_RECORDIMPORT,
                    parameters=dict(date=str(timestamp.date()), hour=timestamp.hour, tenant=tenant))


class RecommendService(object):

    @classmethod
    def compute(cls, date, tenant, algorithm):

        job = ComputeRecommendationTask(date=date, tenant=tenant, algorithm=algorithm)

        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_COMPUTEREC, parameters=dict(date=date, tenant=tenant, algorithm=algorithm))


class DataTrimmingService(object):

    @classmethod
    def trim(cls, date, tenant, starting_date, period):

        job = TrimDataTask(date=date, tenant=tenant, starting_date=starting_date, period=period)

        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_TRIMDATA, parameters=dict(date=date, tenant=tenant, period=period))
