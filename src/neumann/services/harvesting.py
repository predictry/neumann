import datetime

import schedule

from neumann import tasks
from neumann.utils import config
from neumann.utils.logger import Logger


def harvestrecord():

    def inner(tenant):

        now = datetime.datetime.utcnow()
        timestamps = [now - datetime.timedelta(hours=x) for x in range(1, 7*24+1)]

        for timestamp in timestamps:
            tasks.run_record_import_task.delay(timestamp=timestamp, tenant=tenant)

    period = int(config.get('harvester', 'interval'))
    tenant = config.get('tenant', 'name')

    schedule.every(period).seconds.do(inner, tenant=tenant)

    Logger.info(
        'Initiating work harvesting worker for {0}...'.format(
            tenant
        )
    )


def main():

    while True:
        schedule.run_pending()


if __name__ == '__main__':

    # register periodic tasks
    harvestrecord()

    # run tasks
    main()
