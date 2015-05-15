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

if os.name == 'posix':
    tempfile.tempdir = "/tmp"
else:
    tempfile.tempdir = "out"


CSV_EXTENSION = "csv"
JSON_EXTENSION = "json"
VALUE_SEPARATOR = ";"


class TaskRetrieveTenantsItemsList(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()
    start = luigi.IntParameter()
    end = luigi.IntParameter()
    id = luigi.IntParameter()

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(self.date.__str__(), self.__class__.__name__, self.tenant,
                                                 self.id, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`".format(self.__class__.__name__, self.tenant, self.id)

        limit = 500
        skip = 0 + self.start

        filename = self.output().path

        if not os.path.exists(os.path.dirname(filename)):
            os.makedirs(os.path.dirname(filename))

            Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(filename), self.tenant))

        with open(filename, "w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "itemId"])

            while skip < self.end:

                items = StoreService.get_tenant_list_of_items_id(self.tenant, skip, limit)

                for item_id in items:
                    writer.writerow([self.tenant, item_id])

                Logger.info("{0} [Fetched `{1}` item IDs for `{2}`, skipped `{3}`]".format(task, len(items),
                                                                                           self.tenant, skip))

                skip += limit


class TaskComputeRecommendations(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()
    start = luigi.IntParameter()
    end = luigi.IntParameter()
    id = luigi.IntParameter()

    def requires(self):

        return TaskRetrieveTenantsItemsList(date=self.date, tenant=self.tenant, id=self.id, start=self.start,
                                            end=self.end)

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(self.date.__str__(), self.__class__.__name__, self.tenant, self.id,
                                                 CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`".format(self.__class__.__name__, self.tenant, self.id)

        # recommendation_types = ["oipt", "oip", "anon-oip", "oivt", "oiv", "anon-oiv"]
        recommendation_types = ["oipt", "oiv", "anon-oiv"]
        response_items_count = 10

        output_filename = self.output().path
        input_filename = self.input().path

        if not os.path.exists(os.path.dirname(output_filename)):
            os.makedirs(os.path.dirname(output_filename))

            Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(output_filename),
                                                                         self.tenant))

        # compute recommendations
        with open(input_filename, "r") as fpi, open(output_filename, "w") as fpo:

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

                        results = item_based.compute_recommendation(self.tenant, recommendation_types[index], item_id)

                        if len(results) > 0:
                            recommendation_types_used.append(recommendation_types[index])
                            tmp_items.extend(results)

                        index += 1
                        count += len(results)

                    # get item ids
                    items_id = list(set([item["id"] for item in tmp_items]))

                    # register computation
                    writer.writerow([self.tenant, item_id, len(items_id),
                                     VALUE_SEPARATOR.join(recommendation_types_used),
                                     VALUE_SEPARATOR.join(item_id for item_id in items_id)])

                except errors.UnknownRecommendationOption:
                    continue
                except Exception:
                    os.remove(output_filename)
                    raise

        return


class TaskStoreRecommendationResults(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()
    start = luigi.IntParameter()
    end = luigi.IntParameter()
    id = luigi.IntParameter()

    def requires(self):

        return TaskComputeRecommendations(date=self.date, tenant=self.tenant, id=self.id, start=self.start,
                                          end=self.end)

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(self.date.__str__(), self.__class__.__name__, self.tenant, self.id,
                                                 CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`".format(self.__class__.__name__, self.tenant, self.id)

        data_dir = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__, self.tenant,
                                str(self.id))

        input_filename = self.input().path
        output_filename = self.output().path

        # generate files
        s3 = config.get("s3")
        s3bucket = s3["bucket"]
        s3path = os.path.join(s3["folder"], self.tenant, "recommendations")

        if not os.path.exists(os.path.dirname(data_dir)):
            os.makedirs(os.path.dirname(data_dir))

            Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(data_dir), self.tenant))

        try:
            os.mkdir(data_dir)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(data_dir):
                pass
            else:
                raise exc

        with open(input_filename, "r") as fp:

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

                    json.dump(data, f)

        # upload files

        Logger.info("{0} [Running AWS Sync from `{1}` to `{2}/{3}`]".format(task, data_dir, s3bucket, s3path))

        start = time.time()

        aws.sync(data_dir, s3bucket, s3path)

        end = time.time()

        Logger.info("{0} [Finished AWS Sync from `{1}` to `{2}/{3}` in {4}s]".format(task, data_dir, s3bucket, s3path,
                                                                                     end-start))

        # delete generated files
        with open(input_filename, "r") as fpi, open(output_filename, "w") as fpo:

            reader = csv.reader(fpi)
            next(reader)

            writer = csv.writer(fpo, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "itemId", "filename", "filePath"])

            for row in reader:

                _, item_id, _, _, _ = row

                filename = '.'.join([item_id, JSON_EXTENSION])
                file_path = os.path.join(data_dir, filename)

                os.remove(file_path)

                writer.writerow([self.tenant, item_id, filename, file_path])

        return


class TaskRunRecommendationWorkflow(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def requires(self):

        n = StoreService.get_item_count_for_tenant(tenant=self.tenant)

        job_size = 1000
        jobs = list()

        c = 1
        for i in range(0, n, job_size):
            limit = i + job_size
            jobs.append((c, i, limit - 1 if limit < n else n))
            c += 1

        return [TaskStoreRecommendationResults(date=self.date, tenant=self.tenant, id=r[0], start=r[1], end=r[2])
                for r in jobs]

    def output(self):

        file_name = "{0}_{1}_{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "date"])
            writer.writerow([self.tenant, self.date])

        return


class TaskSyncItemsWithS3(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()
    start = luigi.IntParameter()
    end = luigi.IntParameter()
    id = luigi.IntParameter()

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(self.date.__str__(), self.__class__.__name__, self.tenant,
                                                 self.id, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`".format(self.__class__.__name__, self.tenant, self.id)

        data_dir = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__, self.tenant,
                                str(self.id))

        results_filename = self.output().path

        s3 = config.get("s3")
        s3bucket = s3["bucket"]
        s3path = os.path.join(s3["folder"], self.tenant, "items")

        limit = 100
        skip = 0 + self.start

        if not os.path.exists(os.path.dirname(data_dir)):
            os.makedirs(os.path.dirname(data_dir))

            Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(data_dir), self.tenant))

        try:
            os.mkdir(data_dir)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(data_dir):
                pass
            else:
                raise exc

        total = 0
        while skip < self.end:

            _, n = StoreService.download_tenant_items_to_a_folder(self.tenant, data_dir, skip=skip, limit=limit)
            total += n

            Logger.info("{0} [Downloaded `{1}` items, skipping `{2}` for `{3}`]".format(task, n, skip, self.tenant))

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

            writer.writerow([self.tenant, str(total), data_dir])

        return


class TaskRunItemSyncWorkflow(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def requires(self):

        n = StoreService.get_item_count_for_tenant(tenant=self.tenant)

        job_size = 1000
        jobs = list()

        c = 1
        for i in range(0, n, job_size):
            limit = i + job_size
            jobs.append((c, i, limit - 1 if limit < n else n))
            c += 1

        return [TaskSyncItemsWithS3(date=self.date, tenant=self.tenant, id=jobdesc[0], start=jobdesc[1], end=jobdesc[2])
                for jobdesc in jobs]

    def output(self):

        file_name = "{0}_{1}_{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "date"])
            writer.writerow([self.tenant, self.date])

        return


class TaskRetrieveListOfItemCategories(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def output(self):

        file_name = "{0}_{1}_{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`".format(self.__class__.__name__, self.tenant, self.id)

        output_filename = self.output().path

        categories = StoreService.get_tenant_items_categories(self.tenant)

        Logger.info("{0} [Found `{1}` categories for `{2}`]".format(task, len(categories), self.tenant))

        if not os.path.exists(os.path.dirname(output_filename)):
            os.makedirs(os.path.dirname(output_filename))

            Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(output_filename),
                                                                         self.tenant))

        with open(output_filename, "w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "category", "count"])

            for k in categories:

                writer.writerow([self.tenant, k, categories[k]])

        return


class TaskStoreProductsByCategoryOnS3(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()

    def requires(self):

        return TaskRetrieveListOfItemCategories(date=self.date, tenant=self.tenant)

    def output(self):

        file_name = "{0}_{1}_{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`".format(self.__class__.__name__, self.tenant, self.id)

        data_dir = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__, self.tenant)

        input_filename = self.input().path
        output_filename = self.output().path

        s3 = config.get("s3")
        s3bucket = s3["bucket"]
        s3path = os.path.join(s3["folder"], self.tenant, "categories")

        if not os.path.exists(os.path.dirname(data_dir)):
            os.makedirs(os.path.dirname(data_dir))

            Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, os.path.dirname(output_filename),
                                                                         self.tenant))

        try:
            os.mkdir(data_dir)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(data_dir):
                pass
            else:
                raise exc

        categories = dict()

        with open(input_filename, "r") as fp:

            reader = csv.reader(fp)
            next(reader)

            for row in reader:
                _, category, count = row
                categories[category] = count

        for category in categories:

            output_filename = os.path.join(data_dir, '.'.join([category.replace("/", "_"), JSON_EXTENSION]))

            items = StoreService.get_tenant_items_from_category(self.tenant, category, skip=0, limit=100)
            count = categories[category]

            with open(output_filename, "w") as fp:

                json.dump(dict(items=items, count=int(count)), fp)

            categories[category] = len(items)

            Logger.info("{0} [Fetched `{1}` items of category `{2}` from `{3}`]".format(task, len(items), category,
                                                                                        self.tenant))

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

                writer.writerow([self.tenant, category, categories[category]])

        return


if __name__ == "__main__":

    luigi.run()
