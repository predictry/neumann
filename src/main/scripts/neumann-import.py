import argparse
import datetime
import os.path
import luigi
from neumann import workflows

parser = argparse.ArgumentParser(description='This script will tell Neumann to import data from Tapirus')
parser.add_argument('start_date', help='Starting date of data to import')
parser.add_argument('end_date', help='Ending date of data to import')
parser.add_argument('tenant', help='Tenant id to import')
args = parser.parse_args()

start_date = datetime.datetime.strptime(args.start_date, '%Y-%m-%d')
end_date = datetime.datetime.strptime(args.end_date, '%Y-%m-%d')
date = start_date
while date < end_date:
    for hour in range(0, 24):
        print("Processing: date = [{0}], hour=[{1}]\n".format(date, hour))
        timestamp = datetime.datetime.utcnow() - datetime.timedelta(hours=2)
        filepath = os.path.abspath(workflows.__file__)
        classname = workflows.TaskImportRecordIntoNeo4j.__name__
        task = workflows.TaskImportRecordIntoNeo4j(date=date.strftime("%Y-%m-%d"), hour=hour, tenant=args.tenant)
        luigi.build([task], local_scheduler=True)
    date += datetime.timedelta(days=1)
