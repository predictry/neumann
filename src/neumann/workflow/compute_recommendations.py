__author__ = 'guilherme'


import os
import os.path
import errno
import tempfile
import csv
import json

import luigi
import luigi.file
import redis
import redis.exceptions

from neumann.core.tenant import profile
from neumann.core.recommend import item_based
from neumann.core import errors
from neumann.utils import config
from neumann.utils.logger import Logger


tempfile.tempdir = "/tmp"
RESPONSE_ITEMS_COUNT = 10


class TaskRetrieveListOfTenants(luigi.Task):

    date = luigi.DateParameter()

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        #get list of active tenants from graph db store
        active_tenants_names = profile.get_active_tenants_names()

        #write tenants out to file
        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant"])

            for tenant in active_tenants_names:

                writer.writerow([tenant])

        return


class TaskRetrieveTenantsItemsList(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskRetrieveListOfTenants(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = list()

        #get list of tenants
        with self.input().open("r") as f:

            reader = csv.reader(f)
            next(reader)

            for row in reader:

                tenant = row[0]

                tenants.append(tenant)

        #write configuration in output file
        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "item_id"])

            for tenant in tenants:

                items = profile.get_tenant_items_list(tenant)

                for item_id in items:
                    writer.writerow([tenant, item_id])

        return


class TaskComputeRecommendations(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskRetrieveTenantsItemsList(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        rtypes = ["oivt", "oiv", "anon-oiv"]

        #create output dir in temp dir

        output_path = os.path.join(tempfile.gettempdir(), self.date.__str__())

        try:
            os.mkdir(output_path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(output_path):
                pass
            else:
                raise

        #compute recommendations
        with self.input().open("r") as in_file, self.output().open("w") as out_file:

            reader = csv.reader(in_file)
            next(reader)

            writer = csv.writer(out_file, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "item_id", "n_results", "rtypes", "rec_items", "out_file"])

            for row in reader:

                tenant, item_id = row

                try:

                    rtypes_used = list()
                    tmp_items = list()

                    count = 0
                    index = 0

                    while count < RESPONSE_ITEMS_COUNT and index < len(rtypes):

                        results = item_based.compute_recommendation(tenant, rtypes[index], item_id)

                        if len(results) > 0:
                            rtypes_used.append(rtypes[index])
                            tmp_items.extend(results)

                        index += 1
                        count += len(results)

                    #list of unique items
                    items = list()
                    items_id = set([item["id"] for item in tmp_items])

                    for id in items_id:
                        for item in tmp_items:
                            if item["id"] == id:
                                items.append(item)
                                break

                    #save output to its own json file
                    file_name = "{0}_{1}_{2}".format(self.date.__str__(), tenant, item_id)

                    file_path = os.path.join(output_path, file_name)

                    with open(file_path, "w") as f:
                        json.dump(items, f, encoding="UTF-8")

                    #register computation
                    writer.writerow([tenant, item_id, len(items), ';'.join(rtypes_used),
                                    ';'.join(item["id"] for item in items), file_path])

                except errors.UnknownRecommendationOption:
                    continue

        return


class TaskSaveRecommendationResults(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskComputeRecommendations(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        conf = config.load_configuration()
        redis_store = conf["redis-cache"]

        try:

            r_server = redis.Redis(
                host=redis_store["primary"]["ip_address"],
                port=redis_store["primary"]["port"])

        except redis.exceptions.ConnectionError:
            print("Redis failed to connect to '{0}:{1}'".format(redis_store["primary"]["ip_address"],
                                                                redis_store["primary"]["port"]))

        else:

            stats = dict()

            #Upload results
            with self.input().open("r") as in_file:

                reader = csv.reader(in_file)
                next(reader)

                for row in reader:

                    tenant, item_id, n_results, rtypes, rec_items, results_file = row

                    key = ':'.join([tenant, item_id])

                    with open(results_file, "r") as f:

                        data = json.load(f, encoding="UTF-8")

                        if tenant not in stats:
                            stats[tenant] = 0
                        stats[tenant] += 1

                        r_server.set(key, json.dumps(data))

            #delete generated files
            with self.input().open("r") as in_file:

                reader = csv.reader(in_file)
                next(reader)

                for row in reader:

                    tenant, item_id, n_results, rtypes, rec_items, results_file = row

                    try:
                        os.remove(results_file)
                    except OSError as err:
                        Logger.error(err)
                        continue

            with self.output().open("w") as results_file:

                writer = csv.writer(results_file, quoting=csv.QUOTE_ALL)
                writer.writerow(["tenant", "n_items_processed"])

                for k, v in stats.iteritems():

                    writer.writerow([k, v])


#TODO: ADD TASK: UPDATE ITEMS TO S3

#TODO: log success rate for each rtype
#TODO: add task to compute success rate (At least 5 items recommended)
#TODO: compute TR[V, P, AC] recommendations -> Different Workflow


if __name__ == "__main__":

    luigi.run()
