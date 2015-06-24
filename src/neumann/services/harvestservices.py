import datetime

import schedule

from neumann import tasks
from neumann.utils import config
from neumann.utils.logger import Logger


def harvestrecord():

    def inner():

        now = datetime.datetime.utcnow()
        timestamps = [now - datetime.timedelta(hours=x) for x in range(1, 7*24+1)]

        # how do I make sure the timing is right? request for the past two hours
        for timestamp in timestamps:
            # tasks.run_workflow_for_record.delay(timestamp)
            pass

    period = int(config.get('harvester', 'interval'))
    schedule.every(period).seconds.do(inner)

    Logger.info('Initiating work harvesting worker...')


def main():

    while True:
        schedule.run_pending()


if __name__ == '__main__':

    # register periodic tasks
    harvestrecord()

    # run tasks
    main()
