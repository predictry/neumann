__author__ = 'guilherme'


from neumann.core.model import store
from neumann.core.db import rdb
from neumann.utils import config
from neumann.utils.logger import Logger


def get_db_connection_string():

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

    engine = rdb.get_engine(connection_string=get_db_connection_string())

    rows = rdb.select(engine, store.Site)

    sites = []

    for row in rows:

        site = store.Site()
        site.id = row["id"]
        site.name = row["name"]

        sites.append(site)

    return sites




if __name__ == "__main__":

    print(get_tenants())