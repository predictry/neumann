__author__ = 'guilherme'

import os
import os.path
import tempfile
import errno
import json

from neumann.core.model import store
from neumann.core.db import neo4j
from neumann.utils.logger import Logger


class StoreService(object):

    @classmethod
    @neo4j.CypherQuery("MATCH (n :`{LABEL}`) RETURN DISTINCT LABELS(n) AS labels".format(
        LABEL=store.LABEL_ITEM))
    def get_active_tenants(cls, *args, **kwargs):
        """

        :param kwargs:
        :return:
        """

        tenants = []

        for row in kwargs["result"]:
            tenants.extend([x for x in row["labels"] if x != store.LABEL_ITEM])

        return tenants


    @classmethod
    @neo4j.CypherQuery("MATCH (n :`{LABEL}`) WHERE {{tenant}} IN LABELS(n) RETURN COUNT (n) AS n".format(
        LABEL=store.LABEL_ITEM))
    def get_item_count_for_tenant(cls, tenant, *args, **kwargs):
        """

        :param tenant:
        :param kwargs:
        :return:
        """

        return kwargs["result"][0]["n"]

    @classmethod
    @neo4j.CypherQuery("MATCH (n :`{LABEL}` {{id: {{id}} }}) RETURN n".format(LABEL=store.LABEL_ITEM))
    def get_item_node(cls, id, *args, **kwargs):
        """

        :param id:
        :param kwargs:
        :return:
        """

        return kwargs["result"][0]["n"]


    #todo: refactor: retrieve property of entity
    @classmethod
    def get_tenant_list_of_items_id(cls, tenant, skip=0, limit=10):

        q = "MATCH (n :`{LABEL}` :`{TENANT}`) RETURN n.id AS id SKIP {{skip}} LIMIT {{limit}}".format(
            LABEL=store.LABEL_ITEM, TENANT=tenant
        )

        params = [neo4j.Parameter("limit", limit), neo4j.Parameter("skip", skip)]

        query = neo4j.Query(q, params)

        r = neo4j.run_query(query)

        items = [x["id"] for x in r]

        return items

    @classmethod
    def download_tenant_items_to_a_folder(cls, tenant, dir, skip=0, limit=10):

        try:
            os.makedirs(dir)
        except OSError as err:

            if err.errno == errno.EEXIST:
                pass
            else:
                Logger.error(err)
                raise err

        params = [neo4j.Parameter("limit", limit), neo4j.Parameter("skip", skip)]
        q = "MATCH (n :`{LABEL}` :`{TENANT}`) RETURN n AS item SKIP {{skip}} LIMIT {{limit}}".format(
            LABEL=store.LABEL_ITEM, TENANT=tenant
        )

        query = neo4j.Query(q, params)

        r = neo4j.run_query(query, commit=False)

        items = [x["item"].properties for x in r]
        paths = list()

        for item in items:

            file_name = '.'.join([item["id"], "json"])

            tmp_file = os.path.join(dir, file_name)

            paths.append(tmp_file)

            with open(tmp_file, "w") as fp:
                json.dump(item, fp)

        del items[:]

        return paths
