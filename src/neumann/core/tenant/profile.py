__author__ = 'guilherme'

import sqlalchemy
import sqlalchemy.sql

from neumann.core.model import store
from neumann.core.db import rdb
from neumann.core.db import neo4j
from neumann.utils import config
from neumann.utils.logger import Logger


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
    conn = engine.connect()

    s = sqlalchemy.sql.select([store.Site])

    result = conn.execute(s)

    sites = []

    for row in result:

        site = store.Site()
        site.id = row["id"]
        site.name = row["name"]

        sites.append(site)

    return sites


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
def get_active_tenants(**kwargs):
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


@neo4j.CypherQuery("MATCH (n :`{LABEL}`) WHERE {{tenant}} IN LABELS(n) RETURN COUNT (n) AS n".format(LABEL=store.LABEL_ITEM))
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