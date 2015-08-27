import schedule
import time
import datetime
import os.path
import json
from neumann import services
from neumann.utils import config
from neumann.utils.logger import Logger


def import_data():
    Logger.info('Scheduled import data from Neumann is running')
    timestamp = datetime.datetime.utcnow()
    with open(os.path.join(config.PROJECT_BASE, "tenants.json"), "r") as fp:
        tenant_config = json.load(fp)
        for tenant in tenant_config["tenants"]:
            Logger.info('RecordImportService harvest for tenant [{0}] at [{1}]'.format(timestamp, tenant['id']))
            services.RecordImportService.harvest(timestamp, tenant["id"])
            for algo in tenant["algo"]:
                Logger.info(
                    'Computing recommendation for tenant [{0}] at [{1}] using algo [{2}]'.format(
                        tenant, str(timestamp.date()), algo
                    )
                )
                services.ComputeRecommendationTask(str(timestamp.date()), tenant, algo)


def main():
    import_data()
    schedule.every(1).hour.do(import_data)
    while True:
        schedule.run_pending()
        time.sleep(1)

if __name__ == '__main__':
    logging = config.get("logging")
    path = os.path.join(config.PROJECT_BASE, logging["logconfig"])
    Logger.setup_logging(path)

    main()
