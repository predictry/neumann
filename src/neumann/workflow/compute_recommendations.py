__author__ = 'guilherme'


import os
import os.path
import errno
import tempfile
import shutil
import csv
import json
import time

import luigi
import luigi.file

from neumann.core.services import StoreService
from neumann.core.recommend import item_based
from neumann.core import aws
from neumann.core import errors
from neumann.utils import config
from neumann.utils.logger import Logger

#todo: define tempdir in config
if os.name == 'posix':
    tempfile.tempdir = "/tmp"
else:
    tempfile.tempdir = "out"


CSV_EXTENSION = "csv"
JSON_EXTENSION = "json"
VALUE_SEPARATOR = ";"

#todo: implement logging


def task_retrieve_tenant_items_list(tenant, filename):

    task = "Task::RetrieveTenantItemsList"

    n = StoreService.get_item_count_for_tenant(tenant=tenant)

    Logger.info("{0} [Found `{1}` items for `{2}`]".format(task, n, tenant))

    limit = 10000
    skip = 0

    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

        Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(filename), tenant))

    with open(filename, "w") as fp:

        writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
        writer.writerow(["tenant", "itemId"])

        while n > skip:

            items = StoreService.get_tenant_list_of_items_id(tenant, skip, limit)

            for item_id in items:
                writer.writerow([tenant, item_id])

            Logger.info("{0} [Fetched `{1}` item IDs, skipping `{2}` for `{3}`]".format(task, len(items), skip, tenant))

            skip += limit


def task_compute_recommendations_for_tenant(tenant, items_list_filename, output_filename):

    task = "Task::RetrieveComputeRecommendations"

    recommendation_types = ["oipt", "oip", "anon-oip", "oivt", "oiv", "anon-oiv"]
    response_items_count = 10

    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

        Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(output_filename), tenant))

    #compute recommendations
    with open(items_list_filename, "r") as fpi, open(output_filename, "w") as fpo:

        reader = csv.reader(fpi)
        next(reader)

        writer = csv.writer(fpo, quoting=csv.QUOTE_ALL)
        writer.writerow(["tenant", "itemId", "nResults", "recommendationTypes", "recommendedItems"])

        for row in reader:

            _, item_id = row

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

                #Logger.info("{0} [Ran for `{1}`::`{2}` using args(`{3}`)]".format(task, tenant, item_id,
                #                                                                  recommendation_types_used))
            except errors.UnknownRecommendationOption:
                continue
            except Exception:
                os.remove(output_filename)
                raise


def task_store_recommendation_results(tenant, recommendations_filename, data_dir, results_filename):

    task = "Task::StoreRecommendationResults"

    #generate files
    s3 = config.get("s3")
    s3bucket = s3["bucket"]
    s3path = os.path.join(s3["folder"], tenant, "recommendations")

    if not os.path.exists(os.path.dirname(data_dir)):
        os.makedirs(os.path.dirname(data_dir))

        Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(data_dir), tenant))

    try:
        os.mkdir(data_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(data_dir):
            pass
        else:
            raise exc

    with open(recommendations_filename, "r") as fp:

        reader = csv.reader(fp)
        next(reader)

        for row in reader:

            _, item_id, _, recommendation_types, recommended_items = row

            filename = '.'.join([item_id, JSON_EXTENSION])
            file_path = os.path.join(data_dir, filename)

            with open(file_path, "w") as f:
                items = recommended_items.split(VALUE_SEPARATOR)

                if len(items) == 1:
                    if not items[0]:
                        items = list()

                data = dict(items=items, algo=recommendation_types)

                json.dump(data, f, encoding="UTF-8")

    #upload files

    Logger.info("{0} [Running AWS Sync from `{1}` to `{2}/{3}`]".format(task, data_dir, s3bucket, s3path))

    start = time.time()

    aws.sync(data_dir, s3bucket, s3path)

    end = time.time()

    Logger.info("{0} [Finished AWS Sync from `{1}` to `{2}/{3}` in {4}s]".format(task, data_dir, s3bucket, s3path,
                                                                                 end-start))

    #delete generated files

    with open(recommendations_filename, "r") as fpi, open(results_filename, "w") as fpo:

        reader = csv.reader(fpi)
        next(reader)

        writer = csv.writer(fpo, quoting=csv.QUOTE_ALL)
        writer.writerow(["tenant", "itemId", "filename", "filePath"])

        for row in reader:

            _, item_id, _, _, _ = row

            filename = '.'.join([item_id, JSON_EXTENSION])
            file_path = os.path.join(data_dir, filename)

            os.remove(file_path)

            writer.writerow([tenant, item_id, filename, file_path])

    return


def task_sync_items_with_s3(tenant, data_dir, results_filename):

    task = "Task::SyncItemsWithS3"

    s3 = config.get("s3")
    s3bucket = s3["bucket"]
    s3path = os.path.join(s3["folder"], tenant, "items")

    n = StoreService.get_item_count_for_tenant(tenant=tenant)

    Logger.info("{0} [Found `{1}` items for `{2}`]".format(task, n, tenant))

    limit = 5000
    skip = 0

    if not os.path.exists(os.path.dirname(data_dir)):
        os.makedirs(os.path.dirname(data_dir))

        Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(data_dir), tenant))

    try:
        os.mkdir(data_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(data_dir):
            pass
        else:
            raise exc

    while n > skip:

        _, n = StoreService.download_tenant_items_to_a_folder(tenant, data_dir, skip=skip, limit=limit)

        Logger.info("{0} [Downloaded `{1}` items, skipping `{2}` for `{3}`]".format(task, n, skip, tenant))

        skip += limit

    Logger.info("{0} [Running AWS Sync from `{1}` to `{2}/{3}`]".format(task, data_dir, s3bucket, s3path))

    start = time.time()

    aws.sync(data_dir, s3bucket, s3path)

    end = time.time()

    Logger.info("{0} [Finished AWS Sync from `{1}` to `{2}/{3}` in {4}s]".format(task, data_dir, s3bucket, s3path,
                                                                                 end-start))

    shutil.rmtree(data_dir)

    with open(results_filename, "w") as fp:

        writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
        writer.writerow(["tenant", "nItems", "dataDir"])

        writer.writerow([tenant, n, data_dir])

    return


def task_get_tenant_categories(tenant, output_filename):

    task = "Task::RetrieveTenantItemCategories"

    categories = StoreService.get_tenant_items_categories(tenant)

    Logger.info("{0} [Found `{1}` categories for `{2}`]".format(task, len(categories), tenant))

    if not os.path.exists(os.path.dirname(output_filename)):
        os.makedirs(os.path.dirname(output_filename))

        Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(output_filename), tenant))

    with open(output_filename, "w") as fp:

        writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
        writer.writerow(["tenant", "category", "count"])

        for k in categories:

            writer.writerow([tenant, k, categories[k]])

    return


def task_store_tenant_items_by_category_on_s3(tenant, categories_filename, data_dir, output_filename):

    task = "Task::StoreTenantItemsByCategoryOnS3"

    s3 = config.get("s3")
    s3bucket = s3["bucket"]
    s3path = os.path.join(s3["folder"], tenant, "categories")

    if not os.path.exists(os.path.dirname(data_dir)):
        os.makedirs(os.path.dirname(data_dir))

        Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(output_filename), tenant))

    try:
        os.mkdir(data_dir)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(data_dir):
            pass
        else:
            raise exc

    categories = dict()

    with open(categories_filename, "r") as fp:

        reader = csv.reader(fp)
        next(reader)

        for row in reader:
            tenant, category, count = row
            categories[category] = count

    for category in categories:

        output_filename = os.path.join(data_dir, '.'.join([category.replace("/", "_"), JSON_EXTENSION]))

        items = StoreService.get_tenant_items_from_category(tenant, category, skip=0, limit=100)
        count = categories[category]

        with open(output_filename, "w") as fp:

            json.dump(dict(items=items, count=int(count)), fp)

        categories[category] = len(items)

        Logger.info("{0} [Fetched `{1}` items of category `{2}` from `{3}`]".format(task, len(items), category, tenant))

    Logger.info("{0} [Running AWS Sync from `{1}` to `{2}/{3}`]".format(task, data_dir, s3bucket, s3path))

    start = time.time()

    aws.sync(data_dir, s3bucket, s3path)

    end = time.time()

    Logger.info("{0} [Finished AWS Sync from `{1}` to `{2}/{3}` in {4}s]".format(task, data_dir, s3bucket, s3path,
                                                                                 end-start))

    with open(output_filename, "w") as fp:

        writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
        writer.writerow(["tenant", "category", "nItemsUploaded"])

        for category in categories:

            writer.writerow([tenant, category, categories[category]])

    return


class TaskRetrieveTenantsItemsList(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def output(self):

        file_name = "{0}-{1}-{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task_retrieve_tenant_items_list(tenant=self.tenant, filename=self.output().path)

        return


class TaskComputeRecommendations(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def requires(self):

        return TaskRetrieveTenantsItemsList(date=self.date, tenant=self.tenant)

    def output(self):

        file_name = "{0}-{1}-{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task_compute_recommendations_for_tenant(self.tenant, items_list_filename=self.input().path,
                                                output_filename=self.output().path)

        return


class TaskStoreRecommendationResults(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def requires(self):

        return TaskComputeRecommendations(date=self.date, tenant=self.tenant)

    def output(self):

        file_name = "{0}-{1}-{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        data_dir = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__, self.tenant)

        task_store_recommendation_results(tenant=self.tenant, recommendations_filename=self.input().path,
                                          data_dir=data_dir, results_filename=self.output().path)

        return


class TaskSyncItemsWithS3(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def output(self):

        file_name = "{0}-{1}-{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        data_dir = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__, self.tenant)

        task_sync_items_with_s3(tenant=self.tenant, data_dir=data_dir, results_filename=self.output().path)

        return


class TaskRetrieveListOfItemCategories(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def output(self):

        file_name = "{0}-{1}-{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task_get_tenant_categories(tenant=self.tenant, output_filename=self.output().path)

        return


class TaskStoreProductsByCategoryOnS3(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def requires(self):

        return TaskRetrieveListOfItemCategories(date=self.date, tenant=self.tenant)

    def output(self):

        file_name = "{0}-{1}-{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        data_dir = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__, self.tenant)

        task_store_tenant_items_by_category_on_s3(tenant=self.tenant, categories_filename=self.input().path,
                                                  data_dir=data_dir, output_filename=self.output().path)

        return


if __name__ == "__main__":

    luigi.run()

#todo: add logging