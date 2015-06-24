import tempfile
import os
import os.path
import json

import luigi
import luigi.file
import requests

from neumann.core import constants
from neumann.core import aws
from neumann.utils import config
from neumann.utils.logger import Logger


tempfile.tempdir = os.path.join(config.PROJECT_BASE, 'out')
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
            date=str(self.date),
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

                _, stat = aws.S3.download_file(uri, self.output().path)

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
            pass


# class TaskImportRecordIntoNeo4j(luigi.Task):
#
#     tenant = ""
#     date = luigi.DateParameter()
#     hour = luigi.IntParameter()
#
#     def requires(self):
#
#         file_name = "{0}_{1}_{2}_{3}_{4}.{5}".format(self.tenant, self.date.__str__(), self.__class__.__name__, self.tenant, self.id,
#                                                      CSV_EXTENSION)
#
#         file_path = os.path.join(tempfile.gettempdir(), file_name)
#
#         return luigi.LocalTarget(file_path)
#
#     def output(self):
#         pass
#
#     def run(self):

        # if file it not found, but status is OK, delete status of download task

#         pass


if __name__ == '__main__':
    luigi.run()
