import tempfile
import os
import os.path
import json
import time
import datetime

import luigi
import luigi.file
import requests

from neumann.core import constants
from neumann.core import aws
from neumann.core import errors
from neumann.core import parser
from neumann.core.db import neo4j
from neumann.core.transformer import CypherTransformer
from neumann.utils import config
from neumann.utils.logger import Logger
from neumann.utils import io


tempfile.tempdir = os.path.join(config.PROJECT_BASE, 'data/')
JSON_EXTENSION = "JSON"


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

        if not os.path.exists(os.path.dirname(self.output().path)):
            os.makedirs(os.path.dirname(self.output().path))

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

                with open(self.output().path, 'w') as fp:

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

            with open(self.output().path, 'w') as fp:

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


class TaskImportRecordIntoNeo4j(luigi.Task):

    date = luigi.DateParameter()
    hour = luigi.IntParameter()
    tenant = luigi.Parameter()

    def requires(self):

        return TaskDownloadRecord(date=self.date, hour=self.hour, tenant=self.tenant)

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(
            self.__class__.__name__, self.date.__str__(), self.hour, self.tenant, JSON_EXTENSION
        )

        file_path = os.path.join(tempfile.gettempdir(), 'tasks', self.__class__.__name__, file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        if not os.path.exists(os.path.dirname(self.output().path)):
            os.makedirs(os.path.dirname(self.output().path))

        with open(self.input().path, 'r') as fp:

            inp = json.load(fp)

            if inp['status'] == 'NOT_FOUND':
                os.remove(self.input().path)
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

                        os.remove(self.input().path)
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

            with open(self.output().path, 'w') as wp:
                message = 'Records Imported for [tenant={0}, date={1}, hour={2}]: {3}'.format(
                    self.tenant, self.date.__str__(), self.hour, len(inp['records'])
                )
                out = dict(timestamp=datetime.datetime.utcnow(), message=message)
                json.dump(out, wp, cls=io.DateTimeEncoder)


if __name__ == '__main__':
    luigi.run()
