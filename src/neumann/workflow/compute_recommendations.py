__author__ = 'guilherme'

import tempfile
import os.path
import csv

import luigi
import luigi.file

from neumann.core.tenant import profile


class TaskRetrieveListOfTenants(luigi.Task):

    date = luigi.DateParameter()

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)
        file_path = os.path.join(tempfile.gettempdir(), file_name)

        print "PATH: [{0}]".format(file_path)

        with open(file_path, "w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)

            sites = profile.get_tenants()

            for site in sites:

                writer.writerow([site.id, site.name])

if __name__ == "__main__":

    luigi.run()