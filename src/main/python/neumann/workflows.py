import csv
import datetime
import errno
import json
import os
import shutil
import tempfile
import time
import luigi
import requests
import dateutil.tz
import dateutil.parser
import ujson
import random
from neumann import Logger
from neumann.core import constants, aws, errors, parser
from neumann.core.db import neo4j
from neumann.core.recommend import BatchRecommendationProvider
from neumann.core.repository import Neo4jRepository
from neumann.core.transformer import CypherTransformer
from neumann.utils import config, io
from neumann.message.event import EventEmitter


tempfile.tempdir = os.path.expanduser('/var/neumann/data')
if not os.path.exists(tempfile.gettempdir()):
    os.makedirs(tempfile.gettempdir())
JSON_EXTENSION = "json"
CSV_EXTENSION = "csv"
VALUE_SEPARATOR = ";"


class TaskDownloadRecord(luigi.Task):

    date = luigi.DateParameter()
    hour = luigi.IntParameter()
    tenant = luigi.Parameter()

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(
            self.__class__.__name__, self.date.__str__(), self.hour, self.tenant, JSON_EXTENSION
        )

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        host = config.get('tapirus', 'host')
        port = int(config.get('tapirus', 'port'))

        url = 'http://{host}:{port}/records'.format(
            host=host,
            port=port
        )

        params = dict(
            date=self.date.__str__(),
            hour=self.hour,
            tenant=self.tenant
        )

        if not os.path.exists(tempfile.gettempdir()):
            os.makedirs(tempfile.gettempdir())

        self.output().makedirs()

        response = requests.get(url, params)

        # response.
        if response.status_code == constants.HTTP_OK:

            data = response.json()

            status = data['status']

            if status == 'PROCESSED':

                files = []

                if 'record_files' in data:

                    for record in data['record_files']:

                        uri = record['uri']

                        filename = uri.split('/')[-1]
                        filepath = os.path.join(tempfile.gettempdir(), 'tmp', filename)

                        if not os.path.exists(os.path.dirname(filepath)):
                            os.makedirs(os.path.dirname(filepath))

                        _, stat = aws.S3.download_file(uri, filepath)

                        out = dict(path=filepath, uri=uri, s3=dict(status=stat))
                        files.append(out)

                        if stat == constants.HTTP_OK:

                            Logger.info(
                                'Downloaded `{0}` to `{1}`'.format(
                                    uri, filepath
                                )
                            )

                        else:

                            Logger.error(
                                'Got status {0} downloading file from S3: {1}'.format(
                                    stat, uri
                                )
                            )

                else:

                    Logger.info(
                        'Got 0 record files from `[{url}, {date}, {hour}, {tenant}]`'.format(
                            url=url, date=self.hour, hour=self.hour, tenant=self.tenant
                        )
                    )

                with self.output().open('w') as fp:

                    json.dump(dict(records=files, status=status), fp, cls=io.DateTimeEncoder)

            else:
                # building, pending, downloaded: try again later

                Logger.info(
                    'Got 0 record files from `[{url}, {date}, {hour}, {tenant}]`'.format(
                        url=url, date=self.hour, hour=self.hour, tenant=self.tenant
                    )
                )

                return

        elif response.status_code == constants.HTTP_NOT_FOUND:

            data = response.json()

            status = data['status']

            out = dict(records=[], status=status)

            with self.output().open('w') as fp:

                json.dump(out, fp, cls=io.DateTimeEncoder)

        else:
            # unknown problem

            Logger.error(
                'Got status from TAPIRUS {0}:\n{1}'.format(
                    response.status_code,
                    response.content
                )
            )

            raise errors.ProcessFailureError(
                'Got status from TAPIRUS {0}:\n{1}'.format(
                    response.status_code,
                    response.content
                )
            )


class TaskImportRecordIntoNeo4j(luigi.Task, EventEmitter):

    date = luigi.DateParameter()
    hour = luigi.IntParameter()
    tenant = luigi.Parameter()
    job_id = luigi.Parameter(default='default_job_id')

    def requires(self):

        return TaskDownloadRecord(date=self.date, hour=self.hour, tenant=self.tenant)

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(
            self.__class__.__name__, self.date.__str__(), self.hour, self.tenant, JSON_EXTENSION
        )

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        self.output().makedirs()

        with self.input().open('r') as fp:

            inp = json.load(fp)

            if inp['status'] == 'NOT_FOUND':
                self.input().remove()
                return

            for record in inp['records']:

                recordpath = record['path']

                gstart = time.time()*1000

                if recordpath:

                    try:

                        entities = parser.parse_record(recordpath)
                        queries, count, batch_size = [], 0, 1000

                        for entity in entities:

                            if entity.tenant == self.tenant:

                                count += 1

                                cypherqueries = CypherTransformer.transform(entity=entity)

                                queries.extend(cypherqueries)

                                # insert session, agent, user, item, action into neo4j, in batches

                                if count % batch_size == 0:
                                    start = time.time()*1000
                                    neo4j.run_batch_query(queries)
                                    end = time.time()*1000
                                    del queries[:]

                                    Logger.info(
                                        'Batched {0} entries in {1:.2f}ms'.format(
                                            batch_size, end-start
                                        )
                                    )

                        if queries:
                            start = time.time()*1000
                            neo4j.run_batch_query(queries)
                            end = time.time()*1000
                            del queries[:]

                            Logger.info(
                                'Batched {0} entries in {1:.2f}ms'.format(
                                    count % batch_size, end-start
                                )
                            )

                    except FileNotFoundError:

                        self.input().remove()
                        return

                    else:

                        gend = time.time()*1000

                        Logger.info(
                            '{0} ran in {1:.2f}ms'.format(
                                self.__class__.__name__, gend-gstart
                            )
                        )

                    # delete log file
                    os.remove(recordpath)

            with self.output().open('w') as wp:
                message = 'Records Imported for [tenant={0}, date={1}, hour={2}]: {3}'.format(
                    self.tenant, self.date.__str__(), self.hour, len(inp['records'])
                )
                out = dict(timestamp=datetime.datetime.utcnow(), message=message)
                json.dump(out, wp, cls=io.DateTimeEncoder)


class TaskRetrieveTenantsItemsList(luigi.Task):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()
    start = luigi.IntParameter()
    end = luigi.IntParameter()
    id = luigi.IntParameter()
    limit = luigi.IntParameter(default=50000)

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(self.date.__str__(), self.__class__.__name__, self.tenant,
                                                 self.id, CSV_EXTENSION)
        if not os.path.exists(tempfile.gettempdir()):
            os.makedirs(tempfile.gettempdir())
        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`".format(self.__class__.__name__, self.tenant, self.id)
        skip = 0 + self.start
        self.output().makedirs()
        Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, self.output().path, self.tenant))

        count = 0
        expected_count = self.end - self.start
        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "itemId"])

            while count < expected_count:
                items = Neo4jRepository.get_tenant_list_of_items_id(self.tenant, skip, self.limit)
                if not items: break
                if (count + len(items)) > expected_count:
                    items = items[:expected_count-count]
                for item_id in items:
                    writer.writerow([self.tenant, item_id])
                Logger.info("{0} [Fetched `{1}` item IDs for `{2}`, skipped `{3}`]".format(task, len(items),
                                                                                           self.tenant, skip))
                skip += self.limit
                count += len(items)


class TaskComputeRecommendations(luigi.Task):

    algorithm = luigi.Parameter()
    date = luigi.DateParameter()
    tenant = luigi.Parameter()
    start = luigi.IntParameter()
    end = luigi.IntParameter()
    id = luigi.IntParameter()
    whitelist = luigi.Parameter(default=None)
    blacklist = luigi.Parameter(default=None)

    def requires(self):

        return TaskRetrieveTenantsItemsList(date=self.date, tenant=self.tenant, id=self.id, start=self.start,
                                            end=self.end)

    def output(self):

        file_name = "{0}_{1}_{2}_{3}_{4}.{5}".format(self.date.__str__(), self.__class__.__name__, self.tenant,
                                                     self.algorithm, self.id, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`::`{3}`".format(self.__class__.__name__, self.tenant, self.algorithm, self.id)
        batch_size = 50
        blacklist_values = ujson.decode(self.blacklist) if self.blacklist else []
        whitelist_values = ujson.decode(self.whitelist) if self.whitelist else []

        # TODO: read options from Task configuration

        if not os.path.exists(tempfile.gettempdir()):
            os.makedirs(tempfile.gettempdir())

        self.output().makedirs()
        Logger.info("{0} [Created directory `{1}` for `{2}`]".format(task, self.output().path,
                                                                     self.tenant))

        # compute recommendations
        with self.input().open('r') as fpi, self.output().open('w') as fpo:

            reader = csv.reader(fpi)
            next(reader)

            writer = csv.writer(fpo, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "itemId", "nResults", "recommendationType", "recommendedItems"])

            counter = 0
            candidates = []

            def BRP(candidates, writer=writer, tenant=self.tenant, algorithm=self.algorithm):

                results = BatchRecommendationProvider.compute(tenant, algorithm, candidates)

                for i in range(0, len(candidates)):

                    items = []

                    # add whitelist
                    for whitelist_entry in whitelist_values:
                        if random.random() < whitelist_entry['prob']:
                            items.append(whitelist_entry['item'])

                    # get item ids
                    if results[i]:
                        items.extend([item['id'] for item in results[i] if item['id'] not in blacklist_values])
                        items = set(items)

                    # register computation
                    writer.writerow([tenant, candidates[i], len(items),
                                     algorithm,
                                     VALUE_SEPARATOR.join(item_id for item_id in items)])

                del candidates[:]

            for row in reader:

                counter += 1

                _, item_id = row

                candidates.append(item_id)

                if counter % batch_size == 0:

                    BRP(candidates)

            if candidates:

                BRP(candidates)


class TaskStoreRecommendationResults(luigi.Task):

    algorithm = luigi.Parameter()
    date = luigi.DateParameter()
    tenant = luigi.Parameter()
    start = luigi.IntParameter()
    end = luigi.IntParameter()
    id = luigi.IntParameter()
    whitelist = luigi.Parameter(default=None)
    blacklist = luigi.Parameter(default=None)

    def requires(self):

        return TaskComputeRecommendations(date=self.date, tenant=self.tenant, algorithm=self.algorithm,
                                          id=self.id, start=self.start, end=self.end, whitelist=self.whitelist,
                                          blacklist=self.blacklist)

    def output(self):

        file_name = "{0}_{1}_{2}_{3}_{4}.{5}".format(self.date.__str__(), self.__class__.__name__, self.tenant,
                                                     self.algorithm, self.id, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        task = "`{0}`::`{1}`::`{2}`::`{3}`".format(self.__class__.__name__, self.tenant, self.algorithm, self.id)

        data_dir = os.path.join(tempfile.gettempdir(), self.date.__str__(), self.__class__.__name__, self.tenant,
                                self.algorithm, str(self.id))

        if not os.path.exists(tempfile.gettempdir()):
            os.makedirs(tempfile.gettempdir())

        self.output().makedirs()

        # generate files
        s3 = config.get("s3")
        s3bucket = s3["bucket"]
        s3path = os.path.join(s3["folder"], self.tenant, "recommendations", self.algorithm)

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

        with self.input().open('r') as fp:

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
                            continue

                    data = dict(items=items, algo=recommendation_types)

                    json.dump(data, f, cls=io.DateTimeEncoder)

        # upload files

        Logger.info("{0} [Running AWS Sync from `{1}` to `{2}/{3}`]".format(task, data_dir, s3bucket, s3path))

        start = time.time()

        aws.S3.sync(data_dir, s3bucket, s3path)

        # Disable local copy since neumann is moved to a separate server
        # if self.tenant in config.get("output", "targettenants").split():
        #     s3copy = os.path.join('/var/neumann', config.get('output', 'dir'), s3path)
        #     Logger.info("Copying recommendations from {0} to {1}".format(data_dir, s3copy))
        #     copy_tree(data_dir, s3copy)

        end = time.time()

        Logger.info("{0} [Finished AWS Sync from `{1}` to `{2}/{3}` in {4}s]".format(task, data_dir, s3bucket, s3path,
                                                                                     end-start))

        # delete generated files
        with self.input().open('r') as fpi, self.output().open('w') as fpo:
            reader = csv.reader(fpi)
            next(reader)

            writer = csv.writer(fpo, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "itemId", "filename", "filePath"])

            for row in reader:
                _, item_id, _, _, _ = row
                filename = '.'.join([item_id, JSON_EXTENSION])
                file_path = os.path.join(data_dir, filename)
                if os.path.exists(file_path):
                    os.remove(file_path)
                writer.writerow([self.tenant, item_id, filename, file_path])


class TaskRunRecommendationWorkflow(luigi.Task, EventEmitter):

    algorithm = luigi.Parameter()
    date = luigi.DateParameter()
    hour = luigi.IntParameter(default=datetime.datetime.now().time().hour)
    tenant = luigi.Parameter()
    job_size = luigi.IntParameter(default=50000)
    job_id = luigi.Parameter(default='default_job_id')
    whitelist = luigi.Parameter(default=None)
    blacklist = luigi.Parameter(default=None)

    def requires(self):

        n = Neo4jRepository.get_item_count_for_tenant(tenant=self.tenant)
        jobs = []
        c = 1
        for i in range(0, n, self.job_size):
            limit = i + self.job_size
            jobs.append((c, i, limit if limit < n else n))
            c += 1

        return [TaskStoreRecommendationResults(date=self.date, tenant=self.tenant, algorithm=self.algorithm, id=r[0],
                                               start=r[1], end=r[2], whitelist=self.whitelist, blacklist=self.blacklist)
                for r in jobs]

    def output(self):

        file_name = "{0}_{1}_{2}_{3}_{4}.{5}".format(self.date.__str__(), self.hour,
                                                self.__class__.__name__, self.tenant,
                                                self.algorithm, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        if not os.path.exists(tempfile.gettempdir()):
            os.makedirs(tempfile.gettempdir())

        if not os.path.exists(os.path.dirname(self.output().path)):
            os.makedirs(os.path.dirname(self.output().path))

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

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

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

            _, n = Neo4jRepository.download_tenant_items_to_a_folder(self.tenant, data_dir, skip=skip, limit=limit)
            total += n

            Logger.info("{0} [Downloaded `{1}` items, skipping `{2}` for `{3}`]".format(task, n, skip, self.tenant))

            skip += limit

        Logger.info("{0} [Running AWS Sync from `{1}` to `{2}/{3}`]".format(task, data_dir, s3bucket, s3path))

        start = time.time()

        aws.S3.sync(data_dir, s3bucket, s3path)

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

        n = Neo4jRepository.get_item_count_for_tenant(tenant=self.tenant)

        job_size = 1000
        jobs = []

        c = 1
        for i in range(0, n, job_size):
            limit = i + job_size
            jobs.append((c, i, limit - 1 if limit < n else n))
            c += 1

        return [TaskSyncItemsWithS3(date=self.date, tenant=self.tenant, id=jobdesc[0], start=jobdesc[1], end=jobdesc[2])
                for jobdesc in jobs]

    def output(self):

        file_name = "{0}_{1}_{2}.{3}".format(self.date.__str__(), self.__class__.__name__, self.tenant, CSV_EXTENSION)

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        with self.output().open("w") as fp:

            writer = csv.writer(fp, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant", "date"])
            writer.writerow([self.tenant, self.date])

        return


class TaskRunTrimDataWorkflow(luigi.Task, EventEmitter):

    date = luigi.DateParameter()
    tenant = luigi.Parameter()
    starting_date = luigi.DateParameter()
    period = luigi.IntParameter()
    job_id = luigi.Parameter(default='default_job_id')

    def output(self):

        file_name = "{0}_{1}_{2}_{3}_{4}.{5}".format(
            self.__class__.__name__, self.date.__str__(), self.tenant, self.starting_date.__str__(), self.period, 'csv'
        )

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tempdate = self.starting_date - datetime.timedelta(days=self.period)

        cut_off_date = datetime.datetime(
            year=tempdate.year,
            month=tempdate.month,
            day=tempdate.day,
            hour=0,
            minute=0,
            second=0,
            tzinfo=dateutil.tz.gettz('UTC')
        )

        while Neo4jRepository.delete_events_prior_to(self.tenant, cut_off_date.isoformat(), 1000):
            pass

        if not os.path.exists(os.path.dirname(self.output().path)):
            os.makedirs(os.path.dirname(self.output().path))

        with self.output().open('w') as fp:
            fp.write('Ok')


if __name__ == '__main__':
    luigi.run()
