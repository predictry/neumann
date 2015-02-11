__author__ = 'guilherme'

import tempfile
import os.path
import csv

import luigi
import luigi.file

from neumann.core.tenant import profile
from neumann.core.model import store


tempfile.tempdir = "/tmp"


class TaskRetrieveListOfTenants(luigi.Task):

    date = luigi.DateParameter()

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)

            tenants = profile.get_tenants()

            for tenant in tenants:

                writer.writerow([tenant.id, tenant.name])


class TaskFilterNonReadyTenants(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskRetrieveListOfTenants(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = []
        active_tenants = profile.get_active_tenants()

        with self.input().open("r") as f:

            reader = csv.reader(f)

            for row in reader:

                tenant = store.Tenant()
                tenant.id, tenant.name = row

                if tenant.name in active_tenants:
                    tenants.append(tenant)

        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)

            for tenant in tenants:

                writer.writerow([tenant.id, tenant.name])





if __name__ == "__main__":

    luigi.run()
