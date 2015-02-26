__author__ = 'guilherme'


import os
import os.path
import errno
import tempfile
import csv
import json

import luigi
import luigi.file

from neumann.core.tenant import profile
from neumann.core.model import store
from neumann.core.recommend import item_based
from neumann.core import errors


#TODO: insert headers in files
#TODO: remove this line (let the system determine the tmp folder)
tempfile.tempdir = "/tmp"
RESPONSE_ITEMS_COUNT = 10


class TaskRetrieveListOfTenants(luigi.Task):

    date = luigi.DateParameter()

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant_id", "tenant_name"])

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

        tenants = list()

        #get list of active tenants from graph db store
        active_tenants_names = profile.get_active_tenants_names()

        #filter out tenants that aren't active
        with self.input().open("r") as f:

            reader = csv.reader(f)
            next(reader)

            for row in reader:

                tenant = store.Tenant()
                tenant.id, tenant.name = row

                if tenant.name in active_tenants_names:
                    tenants.append(tenant)

        #write tenants out to file
        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant_id", "tenant_name"])

            for tenant in tenants:

                writer.writerow([tenant.id, tenant.name])

        return

'''
class TaskGetTenantsRecommendationSettings(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskFilterOutTenants(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = list()

        #get list of tenants
        with self.input().open("r") as f:

            reader = csv.reader(f)

            for row in reader:

                tenant = store.Tenant()
                tenant.id, tenant.name = row

                tenants.append(tenant)

        #get tenant's configuration
        tenants_recommendation_settings = list()

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
'''


class TaskGetTenantsItemsList(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskFilterOutTenants(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        tenants = list()

        #get list of tenants
        with self.input().open("r") as f:

            reader = csv.reader(f)
            next(reader)

            for row in reader:

                tenant = store.Tenant()
                tenant.id, tenant.name = row

                tenants.append(tenant)

        #write configuration in output file
        with self.output().open("w") as f:

            writer = csv.writer(f, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant_id", "tenant_name", "item_id"])

            for tenant in tenants:

                items = profile.get_tenant_items_list(tenant.name)

                for item_id in items:
                    writer.writerow([tenant.id, tenant.name, item_id])

        return


#TODO: log success rate for each rtype
#TODO: add task to compute success rate (At least 5 items recommended)
#TODO: compute TR[V, P, AC] recommendations -> Different Workflow
class TaskComputeRecommendations(luigi.Task):

    date = luigi.DateParameter()

    def requires(self):

        return TaskGetTenantsItemsList(self.date)

    def output(self):

        file_name = "{0}_{1}".format(self.date, self.__class__.__name__)

        file_path = os.path.join(tempfile.gettempdir(), file_name)

        return luigi.LocalTarget(file_path)

    def run(self):

        rtypes = ["oivt", "oiv", "anon-oiv"]

        #create output dir in temp dir

        output_path = os.path.join(tempfile.gettempdir(), self.date.__str__())

        try:
            os.mkdir(output_path)
        except OSError as exc:
            if exc.errno == errno.EEXIST and os.path.isdir(output_path):
                pass
            else:
                raise

        #compute recommendations
        with self.input().open("r") as in_file, self.output().open("w") as out_file:

            reader = csv.reader(in_file)
            next(reader)

            writer = csv.writer(out_file, quoting=csv.QUOTE_ALL)
            writer.writerow(["tenant_id", "tenant_name", "item_id", "n_results", "rtypes", "rec_items", "out_file"])

            for row in reader:

                tenant_id, tenant_name, item_id = row

                try:

                    rtypes_used = list()
                    tmp_items = list()

                    count = 0
                    index = 0

                    while count < RESPONSE_ITEMS_COUNT and index < len(rtypes):

                        results = item_based.compute_recommendation(tenant_name, rtypes[index], item_id)

                        if len(results) > 0:
                            rtypes_used.append(rtypes[index])
                            tmp_items.extend(results)

                        index += 1
                        count += len(results)

                    #list of unique items
                    items = list()
                    items_id = set([item["id"] for item in tmp_items])

                    for id in items_id:
                        for item in tmp_items:
                            if item["id"] == id:
                                items.append(item)
                                break

                    #save output to its own json file
                    file_name = "{0}_{1}_{2}".format(self.date.__str__(), tenant_name, item_id)

                    file_path = os.path.join(output_path, file_name)

                    with open(file_path, "w") as f:
                        json.dump(tmp_items, f)

                    #register computation
                    writer.writerow([tenant_id, tenant_name, item_id, len(items), ';'.join(rtypes_used),
                                    ';'.join(item["id"] for item in items), file_path])

                except errors.UnknownRecommendationOption:
                    continue

        return


if __name__ == "__main__":

    luigi.run()
