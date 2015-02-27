__author__ = 'guilherme'

import sqlalchemy
import sqlalchemy.sql

from neumann.core.model import store
from neumann.core.db import rdb
from neumann.core.db import neo4j
from neumann.utils import config
from neumann.utils.logger import Logger


class TenantRecommendationSetting(object):

    def __init__(self, tenant, method):
        self.tenant = tenant
        self.method = method


def get_db_connection_string():
    """

    :return:
    """

    #todo: configuration should be a class, not a dictionary
    #todo: build unit test
    configuration = config.load_configuration()

    if "client-db" in configuration:

        try:

            db_conf = configuration["client-db"]

            s = "{DIALECT}{DRIVER}://{USERNAME}:{PASSWORD}@{HOST}/{DATABASE}".format(
                DIALECT=db_conf["dialect"],
                DRIVER=''.join(["+", db_conf["driver"]]) if db_conf["driver"] else "",
                USERNAME=db_conf["username"],
                PASSWORD=db_conf["password"],
                HOST=db_conf["host"],
                DATABASE=db_conf["database"])

        except KeyError as err:

            Logger.error("Error reading Client DB configuration:\n\t{0}".format(err))
        else:

            return s

    else:

        raise KeyError("Client DB configuration is missing from configuration file. `{0}` key was not found".format(
            "client-db"
        ))


def get_tenants():
    """

    :return:
    """

    engine = rdb.get_engine(connection_string=get_db_connection_string())

    try:
        conn = engine.connect()

    except Exception as err:

        Logger.error(err)

        raise err

    else:

        s = sqlalchemy.sql.select([store.Tenant])

        result = conn.execute(s)

        tenants = []

        for row in result:

            tenant = store.Tenant()
            tenant.id = row["id"]
            tenant.name = row["name"]

            tenants.append(tenant)

        return tenants


def get_tenant_widgets(tenant_id):
    """

    :return:
    """

    engine = rdb.get_engine(connection_string=get_db_connection_string())
    conn = engine.connect()

    s = sqlalchemy.sql.select([store.Widget]).where(store.Widget.site_id == tenant_id)

    result = conn.execute(s)

    widgets = []

    for row in result:

        widget = store.Widget()
        widget.id = row["id"]
        widget.site_id = row["site_id"]
        widget.reco_type = row["reco_type"]

        widgets.append(widget)

    return widgets


@neo4j.CypherQuery("MATCH (n :`{LABEL}`) RETURN DISTINCT LABELS(n) AS labels".format(
    LABEL=store.LABEL_ITEM))
def get_active_tenants_names(**kwargs):
    """

    :param kwargs:
    :return:
    """

    tenants = []

    for row in kwargs["result"]:
        tenants.extend([x for x in row["labels"] if x != store.LABEL_ITEM])

    return tenants


@neo4j.CypherQuery("MATCH (n :`{LABEL}`) RETURN COUNT (n) AS n".format(LABEL=store.LABEL_ITEM))
def item_count(**kwargs):
    """

    :param kwargs:
    :return:
    """

    return kwargs["result"][0]["n"]


@neo4j.CypherQuery("MATCH (n :`{LABEL}`) WHERE {{tenant}} IN LABELS(n) RETURN COUNT (n) AS n".format(
    LABEL=store.LABEL_ITEM))
def item_count_for_tenant(tenant, **kwargs):
    """

    :param tenant:
    :param kwargs:
    :return:
    """

    return kwargs["result"][0]["n"]


@neo4j.CypherQuery("MATCH (n :`{LABEL}` {{id: {{id}} }}) RETURN n".format(LABEL=store.LABEL_ITEM))
def get_item(id, **kwargs):
    """

    :param id:
    :param kwargs:
    :return:
    """

    return kwargs["result"][0]["n"]


def get_tenant_items_list(tenant):

    n = item_count_for_tenant(tenant=tenant)

    limit = 50000
    skip = 0

    q = "MATCH (n :`{LABEL}` :`{TENANT}`) RETURN n.id AS id SKIP {{skip}} LIMIT {{limit}}".format(
        LABEL=store.LABEL_ITEM, TENANT=tenant
    )

    items = list()

    while n > skip:

        params = [neo4j.Parameter("limit", limit), neo4j.Parameter("skip", skip)]

        query = neo4j.Query(q, params)

        r = neo4j.run_query(query, commit=False)

        items.extend([x["id"] for x in r])

        skip += limit

    return items


