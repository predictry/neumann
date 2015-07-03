from neumann.core import errors
from neumann import tasks

from neumann.tasks import RecordImportTask
from neumann.tasks import ComputeRecommendationTask


class ServiceUseCases(object):

    @classmethod
    def queuetask(cls, args):

        if 'name' not in args:
            raise errors.MissingParameterError(
                'Parameter `name` is missing'
            )

        if 'tenant' not in args:
            raise errors.MissingParameterError(
                'Parameter `tenant` is missing'
            )

        if args['name'] not in tasks.TASK_TYPES:
            raise errors.UnknownTaskError(
                'Unknown task `{0}`'.format(
                    args['name']
                )
            )


class RecordImportUseCases(object):

    @classmethod
    def harvest(cls, timestamp, tenant):

        job = RecordImportTask(timestamp=timestamp, tenant=tenant)

        # queue task
        job.run()


class ComputeRecommendationUseCases(object):

    @classmethod
    def compute(cls, date, tenant):

        job = ComputeRecommendationTask(date=date, tenant=tenant)

        job.run()
