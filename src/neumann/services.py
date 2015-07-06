from neumann import tasks
from neumann.tasks import RecordImportTask
from neumann.tasks import ComputeRecommendationTask
from neumann.utils import io
from neumann.utils.logger import Logger


class RecordImportService(object):

    @classmethod
    def harvest(cls, timestamp, tenant):

        job = RecordImportTask(timestamp=timestamp, tenant=tenant)

        # queue task
        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_HARVESTDATA, date=str(timestamp.date()), hour=timestamp.hour, tenant=tenant)


class RecommendService(object):

    @classmethod
    def compute(cls, date, tenant):

        job = ComputeRecommendationTask(date=date, tenant=tenant)

        job.run()

        Logger.info('Queued {0}'.format(job))

        return dict(task=tasks.TASK_TYPE_COMPUTEREC, date=date, tenant=tenant)
