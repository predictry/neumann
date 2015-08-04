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

        return dict(task=tasks.TASK_TYPE_RECORDIMPORT, date=str(timestamp.date()), hour=timestamp.hour, tenant=tenant)


class RecommendService(object):

    @classmethod
    def compute(cls, date, tenant):

        job = ComputeRecommendationTask(date=date, tenant=tenant)

        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_COMPUTEREC, date=date, tenant=tenant)


class DataTrimmingService(object):

    @classmethod
    def trim(cls, date, tenant, starting_date, period):

        job = TrimDataTask(date=date, tenant=tenant, starting_date=starting_date, period=period)

        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_TRIMDATA, date=date, tenant=tenant, period=period)
