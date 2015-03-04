__author__ = 'guilherme'


import os
import os.path
import errno
import tempfile
import csv
import json
import shutil

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
        with self.input().open("r") as fp:

            reader = csv.reader(fp)
            next(reader)

            for row in reader:

                tenant, = row

                tenants.append(tenant)

        #write configuration in output file
        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "item_id"])

            for tenant in tenants:

                items = profile.get_tenant_list_of_items_id(tenant)

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
                raise exc

        #compute recommendations
        with self.input().open("r") as in_file, self.output().open("w") as output_file:

            reader = csv.reader(in_file)
            next(reader)

            writer = csv.writer(output_file, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "item_id", "n_results", "rtypes", "rec_items", "output_file"])

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

                    #get item ids
                    items_id = list(set([item["id"] for item in tmp_items]))

                    #save output to its own json file
                    file_name = "{0}_{1}_{2}".format(self.date.__str__(), tenant, item_id)

                    file_path = os.path.join(output_path, file_name)

                    with open(file_path, "w") as f:
                        json.dump(items_id, f, encoding="UTF-8")

                    #register computation
                    writer.writerow([tenant, item_id, len(items_id), ';'.join(rtypes_used),
                                    ';'.join(item_id for item_id in items_id), file_path])

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
        redis_store = conf["redis"]

        r_server = redis.Redis(
            host=redis_store["host"],
            port=redis_store["port"])

        stats = dict()

        with self.input().open("r") as in_file:

            reader = csv.reader(in_file)
            next(reader)

            for row in reader:

                tenant, item_id, n_results, rtypes, rec_items, file_path = row

                key = ':'.join([tenant, item_id])

                with open(file_path, "r") as f:

                    data = json.load(f, encoding="UTF-8")

                    if tenant not in stats:
                        stats[tenant] = 0
                    stats[tenant] += 1

                    try:
                        r_server.set(key, json.dumps(data))
                    except redis.exceptions.ConnectionError as exc:
                        Logger.error("Redis failed to connect to '{0}:{1}'".format(redis_store["host"],
                                                                                   redis_store["port"]))
                        raise exc

        with self.input().open("r") as in_file:

            reader = csv.reader(in_file)
            next(reader)

            for row in reader:

                tenant, item_id, n_results, rtypes, rec_items, file_path = row

                try:
                    os.remove(file_path)
                except OSError as err:
                    Logger.error(err)
                    continue

        with self.output().open("w") as file_path:

            writer = csv.writer(file_path, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "n_items_processed"])

            for k, v in stats.iteritems():

                writer.writerow([k, v])


class TaskDownloadTenantsItemsToLocalFolder(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskRetrieveListOfTenants(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenant_folders = dict()

        with self.input().open("r") as fp:

            reader = csv.reader(fp)
            next(reader)

            for row in reader:

                tenant = row[0]

                path = profile.download_tenant_items_to_a_folder(tenant)

                tenant_folders[tenant] = path

        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "items_folder"])

            for k, v in tenant_folders.iteritems():

                writer.writerow([k, v])


class TaskUploadTenantsItemsToS3(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskDownloadTenantsItemsToLocalFolder(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        conf = config.load_configuration()
        s3_config = conf["s3"]

        bucket = s3_config["bucket"]
        s3_folder = s3_config["folder"]

        with self.input().open("r") as fpi, self.output().open("w") as fpo:

            reader = csv.reader(fpi)
            next(reader)

            writer = csv.writer(fpo)
            writer.writerow(["tenant", "state"])

            for row in reader:
                tenant, items_folder = row

                try:
                    profile.sync_tenant_items_to_s3(tenant, bucket,s3_folder, items_folder)
                except RuntimeError as exc:
                    raise exc
                else:
                    writer.writerow([tenant, "processed"])

        with self.input().open("r") as fp:

            reader = csv.reader(fp)
            next(reader)

            for row in reader:
                tenant, items_folder = row

                try:
                    shutil.rmtree(items_folder)
                except OSError as err:
                    Logger.error(err)
                    raise err


if __name__ == "__main__":

    luigi.run()
