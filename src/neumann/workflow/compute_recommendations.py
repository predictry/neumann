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

        return


class TaskFilterOutTenants(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskRetrieveListOfTenants(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = []

        #get list of active tenants from graph db store
        active_tenants = profile.get_active_tenants()

        #filter out tenants that arent' active
        with self.input().open("r") as f:

            reader = csv.reader(f)

            for row in reader:

                tenant = store.Tenant()
                tenant.id, tenant.name = row

                if tenant.name in active_tenants:
                    tenants.append(tenant)

        #write tenants out to file
        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)

            for tenant in tenants:

                writer.writerow([tenant.id, tenant.name])

        return


class TaskGetTenantsRecommendationSettings(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskFilterOutTenants(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = []

        #get list of tenants
        with self.input().open("r") as f:

            reader = csv.reader(f)

            for row in reader:

                tenant = store.Tenant()
                tenant.id, tenant.name = row

                tenants.append(tenant)

        #get tenant's configuration
        tenants_recommendation_settings = []

        for tenant in tenants:

            widgets = profile.get_tenant_widgets(int(tenant.id))

            methods = set([widget.reco_type for widget in widgets])

            tenants_recommendation_settings.extend(
                [profile.TenantRecommendationSetting(tenant, method) for method in methods])

        #write configuration in output file
        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)

            for setting in tenants_recommendation_settings:

                writer.writerow([setting.tenant.id, setting.tenant.name,
                                 setting.method])

        return


if __name__ == "__main__":

    luigi.run()
