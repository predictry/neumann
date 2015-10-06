import schedule
import time
import datetime
import json
from neumann import services
from neumann.utils.config import TENANTS_CONFIG_FILE, LOGGING_CONFIG_FILE
from neumann.utils.logger import Logger


def import_data():
    Logger.info('Scheduled import data from Neumann is running')
    timestamp = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
    with open(TENANTS_CONFIG_FILE, "r") as fp:
        tenant_config = json.load(fp)
        for tenant in tenant_config["tenants"]:
            Logger.info('RecordImportService harvest for tenant [{0}] at [{1}]'.format(timestamp, tenant['id']))
            services.RecordImportService.harvest(timestamp, tenant["id"])
            for algo in tenant["algo"]:
                Logger.info(
                    'Computing recommendation for tenant [{0}] at [{1}] using algo [{2}]'.format(
                        tenant['id'], str(timestamp.date()), algo
                    )
                )
                services.RecommendService.compute(str(timestamp.date()), tenant['id'], algo)


def main():
    import_data()
    schedule.every(1).hour.do(import_data)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    Logger.setup_logging(LOGGING_CONFIG_FILE)

    main()
