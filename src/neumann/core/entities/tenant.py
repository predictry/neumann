__author__ = 'guilherme'

import os
import os.path
import sys
import tempfile
import errno
import json
import subprocess
import sqlalchemy
import sqlalchemy.sql

from neumann.core.model import store
from neumann.core.db import rdb
from neumann.core.db import neo4j
from neumann.utils import config
from neumann.utils.logger import Logger


@neo4j.CypherQuery("MATCH (n :`{LABEL}`) RETURN DISTINCT LABELS(n) AS labels".format(
    LABEL=store.LABEL_ITEM))
def get_active_tenants(**kwargs):
    """

    :param kwargs:
    :return:
    """

    tenants = []

    for row in kwargs["result"]:
        tenants.extend([x for x in row["labels"] if x != store.LABEL_ITEM])

    return tenants


@neo4j.CypherQuery("MATCH (n :`{LABEL}`) WHERE {{tenant}} IN LABELS(n) RETURN COUNT (n) AS n".format(
    LABEL=store.LABEL_ITEM))
def get_item_count_for_tenant(tenant, **kwargs):
    """

    :param tenant:
    :param kwargs:
    :return:
    """

    return kwargs["result"][0]["n"]


@neo4j.CypherQuery("MATCH (n :`{LABEL}` {{id: {{id}} }}) RETURN n".format(LABEL=store.LABEL_ITEM))
def get_item_node(id, **kwargs):
    """

    :param id:
    :param kwargs:
    :return:
    """

    return kwargs["result"][0]["n"]


#todo: refactor: retrieve property of entity
def get_tenant_list_of_items_id(tenant, skip=0, limit=10):

    q = "MATCH (n :`{LABEL}` :`{TENANT}`) RETURN n.id AS id SKIP {{skip}} LIMIT {{limit}}".format(
        LABEL=store.LABEL_ITEM, TENANT=tenant
    )

    params = [neo4j.Parameter("limit", limit), neo4j.Parameter("skip", skip)]

    query = neo4j.Query(q, params)

    r = neo4j.run_query(query)

    items = [x["id"] for x in r]

    return items


def download_tenant_items_to_a_folder(tenant):

    #TODO: implement paging (tenant, limit, skip)
    n = get_item_count_for_tenant(tenant=tenant)

    limit = 30000
    skip = 0

    q = "MATCH (n :`{LABEL}` :`{TENANT}`) RETURN n AS item SKIP {{skip}} LIMIT {{limit}}".format(
        LABEL=store.LABEL_ITEM, TENANT=tenant
    )

    data_folder = os.path.join(tempfile.gettempdir(), '/'.join(["tenants", tenant, "items"]))

    try:

        os.makedirs(data_folder)

    except OSError as err:

        if err.errno == errno.EEXIST:
            pass
        else:
            Logger.error(err)
            raise err

    while n > skip:

        params = [neo4j.Parameter("limit", limit), neo4j.Parameter("skip", skip)]

        query = neo4j.Query(q, params)

        r = neo4j.run_query(query, commit=False)

        items = [x["item"].properties for x in r]

        for item in items:

            file_name = ''.join([item["id"], ".json"])

            tmp_file = os.path.join(data_folder, file_name)

            with open(tmp_file, "w") as fp:
                json.dump(item, fp)

        skip += limit

        del items[:]

    return data_folder


def sync_tenant_items_to_s3(tenant, bucket, s3_folder, local_folder):

    s3path = ''.join(["s3://", '/'.join([bucket, s3_folder, tenant, "items"])])

    awscli = os.path.join(os.path.abspath(os.path.join(sys.executable, os.pardir)), "aws")
    cmd = [awscli, "s3", "sync", local_folder, s3path]

    Logger.info("Running command:\t{0}".format(''.join(cmd)))

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, shell=False)

    output, err = p.communicate()

    if p.returncode == 1:

        msg = "Error running command:\n\t{0}".format(' '.join(cmd))
        Logger.error(msg)
        Logger.error(err)

        raise RuntimeError(err)

    elif p.returncode == 0:

        Logger.info("Successfully executed command:\n\t{0}".format(' '.join(cmd)))

    return