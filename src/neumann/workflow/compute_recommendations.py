__author__ = 'guilherme'


import os
import os.path
import errno
import tempfile
import csv
import json
import multiprocessing

import luigi
import luigi.file

from neumann.core.entities import tenant as Tenant
from neumann.core.recommend import item_based
from neumann.core import aws
from neumann.core import errors
from neumann.utils import config


if os.name == 'posix':
    tempfile.tempdir = "/tmp"
else:
    tempfile.tempdir = "out"


CSV_EXTENSION = "csv"
JSON_EXTENSION = "json"
VALUE_SEPARATOR = ";"


def task_retrieve_tenant_items_list(tenant, filename, output_queue):

    n = Tenant.get_item_count_for_tenant(tenant=tenant)

    limit = 10000
    skip = 0

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    with open(filename, "w") as fp:

        writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
        writer.writerow(["tenant", "item_id"])

        while n > skip:

            items = Tenant.get_tenant_list_of_items_id(tenant, skip, limit)

            for item_id in items:
                writer.writerow([tenant, item_id])

            skip += limit

    output_queue.put((tenant, filename))


def task_compute_recommendations_for_tenant(items_list_filename, results_log_dir):

    recommendation_types = ["oipt", "oip", "anon-oip", "oivt", "oiv", "anon-oiv"]
    response_items_count = 10

    if not os.path.exists(os.path.dirname(results_log_dir)):
        os.makedirs(os.path.dirname(results_log_dir))

    #compute recommendations
    with open(items_list_filename, "r") as fpi, open(results_log_dir, "w") as fpo:

        reader = csv.reader(fpi)
        next(reader)

        writer = csv.writer(fpo, quoting=csv.QUOTE_ALL)
        writer.writerow(["tenant", "item_id", "n_results", "recommendation_types", "recommended_items"])

        for row in reader:

            tenant, item_id = row

            try:

                recommendation_types_used = list()
                tmp_items = list()

                count = 0
                index = 0

                while count < response_items_count and index < len(recommendation_types):

                    results = item_based.compute_recommendation(tenant, recommendation_types[index], item_id)

                    if len(results) > 0:
                        recommendation_types_used.append(recommendation_types[index])
                        tmp_items.extend(results)

                    index += 1
                    count += len(results)

                #get item ids
                items_id = list(set([item["id"] for item in tmp_items]))

                #register computation
                writer.writerow([tenant, item_id, len(items_id), VALUE_SEPARATOR.join(recommendation_types_used),
                                VALUE_SEPARATOR.join(item_id for item_id in items_id)])

            except errors.UnknownRecommendationOption:
                continue


def task_store_recommendation_results(tenant, results_filename, data_dir):

    #generate files
    s3 = config.load_configuration()["s3"]
    s3bucket = s3["bucket"]
    s3path = os.path.join(s3["folder"], tenant, "recommendations")

    tenant_items_recommendation_dir = os.path.join(data_dir, tenant)

    if not os.path.exists(os.path.dirname(tenant_items_recommendation_dir)):
        os.makedirs(os.path.dirname(tenant_items_recommendation_dir))

    try:
        os.mkdir(tenant_items_recommendation_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(tenant_items_recommendation_dir):
            pass
        else:
            raise exc

    with open(results_filename, "r") as fp:

        reader = csv.reader(fp)
        next(reader)

        for row in reader:

            te, item_id, n_results, recommendation_types, recommended_items = row

            filename = '.'.join([item_id, JSON_EXTENSION])
            file_path = os.path.join(tenant_items_recommendation_dir, filename)

            with open(file_path, "w") as f:
                items = recommended_items.split(VALUE_SEPARATOR)

                if len(items) == 1:
                    if not items[0]:
                        items = list()

                json.dump(items, f, encoding="UTF-8")

    #upload files

    aws.sync_tenant_items_to_s3(tenant_items_recommendation_dir, s3bucket, s3path)

    #delete generated files

    with open(results_filename, "r") as fp:

        reader = csv.reader(fp)
        next(reader)

        for row in reader:

            te, item_id, n_results, recommendation_types, recommended_items = row

            filename = '.'.join([item_id, JSON_EXTENSION])
            file_path = os.path.join(tenant_items_recommendation_dir, filename)

            os.remove(file_path)

    return


class TaskRetrieveListOfTenants(luigi.Task):

    date = luigi.DateParameter()

    def output(self):

        file_name = "{0}_{1}.{2}".format(self.date.__str__(), self.__class__.__name__, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        #get list of active tenants from graph db store

        active_tenants_names = Tenant.get_active_tenants()

        if not os.path.exists(os.path.dirname(self.output().path)):
            os.makedirs(os.path.dirname(self.output().path))

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

        file_name = "{0}_{1}.{2}".format(self.date.__str__(), self.__class__.__name__, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = list()
        jobs = list()
        cpu_count = multiprocessing.cpu_count()
        output_queue = multiprocessing.Queue()

        #get list of tenants
        with self.input().open("r") as fp:

            reader = csv.reader(fp)
            next(reader)

            for row in reader:

                tenant, = row

                tenants.append(tenant)

        for tenant in tenants:

            filename = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__,
                                    '.'.join([tenant, CSV_EXTENSION]))

            job = multiprocessing.Process(target=task_retrieve_tenant_items_list, args=(tenant, filename,
                                                                                        output_queue))

            jobs.append(job)

        if cpu_count > 1:

            while jobs:

                upper_bound = cpu_count - 1 if len(jobs) > cpu_count else len(jobs) - 1

                #there is only one job left
                if upper_bound == 0:
                    upper_bound = 1

                for job in jobs[0:upper_bound]:
                    job.start()

                for job in jobs[0:upper_bound]:
                    job.join()

                del jobs[0:upper_bound]
        else:

            for job in jobs:
                job.start()
                job.join()

        if not os.path.exists(os.path.dirname(self.output().path)):
            os.makedirs(os.path.dirname(self.output().path))

        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "items_list_file"])

            while not output_queue.empty():

                r = output_queue.get()
                writer.writerow([r[0], r[1]])

        return


class TaskComputeRecommendations(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskRetrieveTenantsItemsList(self.date)

    def output(self):

        file_name = "{0}_{1}.{2}".format(self.date.__str__(), self.__class__.__name__, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = dict()
        jobs = list()
        cpu_count = multiprocessing.cpu_count()

        #create output dir in temp dir

        output_path = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__)

        try:
            os.mkdir(output_path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(output_path):
                pass
            else:
                raise exc

        with self.input().open("r") as fp:

            reader = csv.reader(fp)
            next(reader)

            for row in reader:
                tenant, items_list_filename = row

                tenants[tenant] = items_list_filename

        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "results_log_file"])

            for k, v in tenants.iteritems():
                tenant, items_list_filename = k, v

                results_log_file = os.path.join(output_path, '.'.join([tenant, CSV_EXTENSION]))

                job = multiprocessing.Process(target=task_compute_recommendations_for_tenant, args=(items_list_filename,
                                                                                                    results_log_file))
                jobs.append(job)

            #compute recommendations
            if cpu_count > 1:

                while jobs:

                    upper_bound = cpu_count - 1 if len(jobs) > cpu_count else len(jobs) - 1

                    #there is only one job left
                    if upper_bound == 0:
                        upper_bound = 1

                    for job in jobs[0:upper_bound]:
                        job.start()

                    for job in jobs[0:upper_bound]:
                        job.join()

                    del jobs[0:upper_bound]
            else:

                for job in jobs:
                    job.start()
                    job.join()

            for k, v in tenants.iteritems():
                tenant, items_list_filename = k, v

                results_log_file = os.path.join(output_path, '.'.join([tenant, CSV_EXTENSION]))

                writer.writerow([tenant, results_log_file])

        return


class TaskStoreRecommendationResults(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskComputeRecommendations(self.date)

    def output(self):

        file_name = "{0}_{1}.{2}".format(self.date.__str__(), self.__class__.__name__, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = dict()

        jobs = list()
        cpu_count = multiprocessing.cpu_count()
        data_dir = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__)

        with self.input().open("r") as fp:

            reader = csv.reader(fp)
            next(reader)

            for row in reader:
                tenant, results_log_file = row

                tenants[tenant] = results_log_file

        for k, v in tenants.iteritems():

            tenant, results_log_file = k, v

            job = multiprocessing.Process(target=task_store_recommendation_results, args=(tenant, results_log_file,
                                                                                          data_dir))
            jobs.append(job)

        if cpu_count > 1:

            while jobs:

                upper_bound = cpu_count - 1 if len(jobs) > cpu_count else len(jobs) - 1

                #there is only one job left
                if upper_bound == 0:
                    upper_bound = 1

                for job in jobs[0:upper_bound]:
                    job.start()

                for job in jobs[0:upper_bound]:
                    job.join()

                del jobs[0:upper_bound]
        else:

            for job in jobs:
                job.start()
                job.join()

        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant"])

            for k, v in tenants.iteritems():

                tenant, results_log_file = k, v

                writer.writerow([tenant, results_log_file])


'''
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

                path = tenant.download_tenant_items_to_a_folder(tenant)

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
                    tenant.sync_tenant_items_to_s3(tenant, bucket, s3_folder, items_folder)
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
'''

if __name__ == "__main__":

    luigi.run()
