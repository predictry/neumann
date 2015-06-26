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


tempfile.tempdir = os.path.join(config.PROJECT_BASE, 'data/out')
JSON_EXTENSION = "json"


class TaskDownloadRecord(luigi.Task):

    date = luigi.DateParameter()
    hour = luigi.IntParameter()

    def output(self):

        file_name = "{0}_{1}_{2}.{3}".format(
            self.__class__.__name__, self.date.__str__(), self.hour, JSON_EXTENSION
        )

        file_path = os.path.join(tempfile.gettempdir(), file_name)

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
            hour=self.hour
        )

        if not os.path.exists(tempfile.gettempdir()):
            os.makedirs(tempfile.gettempdir())

        response = requests.get(url, params)

        # response.
        if response.status_code == constants.HTTP_OK:

            data = response.json()

            status = data['status']

            if status == 'PROCESSED':

                uri = data['uri']

                filename = uri.split('/')[-1]
                filepath = os.path.join(tempfile.gettempdir(), filename)

                _, stat = aws.S3.download_file(uri, filepath)

                if stat == constants.HTTP_OK:

                    out = dict(path=filepath, uri=uri, status=status)

                    with open(self.output().path, 'w') as fp:

                        json.dump(out, fp)

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
                # building, pending, downloaded: try again later
                return

        elif response.status_code == constants.HTTP_NOT_FOUND:

            data = response.json()

            status = data['status']
            uri = data['uri']

            out = dict(path=None, uri=uri, status=status)

            with open(self.output().path, 'w') as fp:

                json.dump(out, fp)

        else:
            # unknown problem

            Logger.error(
                'Got status from tapirus {0}:\n{1}'.format(
                    response.status_code,
                    response.content
                )
            )

            raise errors.ProcessFailureError(
                'Got status from tapirus {0}:\n{1}'.format(
                    response.status_code,
                    response.content
                )
            )

class TaskImportRecordIntoNeo4j(luigi.Task):

    tenant = luigi.Parameter()
    date = luigi.DateParameter()
    hour = luigi.IntParameter()

    def requires(self):

        return TaskDownloadRecord(date=self.date, hour=self.hour)

    def output(self):

        file_name = "{0}_{1}_{2}_{3}.{4}".format(
            self.__class__.__name__, self.date.__str__(), self.hour,  self.tenant, JSON_EXTENSION
        )

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        # TODO: Use RQ priorities. Computation takes precedence over other queues

        with open(self.input().path, 'r') as fp:

            inp = json.load(fp)
            recordpath = inp['path']

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

                os.remove(recordpath)

                with open(self.output().path, 'w') as wp:
                    out = dict(timestamp=datetime.datetime.utcnow())
                    json.dump(out, wp)

            else:

                # 404
                if inp['status'] == 'NOT_FOUND':
                    os.remove(self.input().path)
                    return


if __name__ == '__main__':
    luigi.run()
