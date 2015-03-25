__author__ = 'guilherme'

import re

from neo4jrestclient.client import GraphDatabase, Node, Relationship
from neo4jrestclient import exceptions
import neo4jrestclient.options

from neumann.core import errors
from neumann.utils import config
from neumann.utils.logger import Logger

#note: do not reuse connections: timeout, re-connection slows down everything for some reason
NEO_VAR_NAME_LABEL_REGEX = "^[a-zA-Z_][a-zA-Z0-9_]*$"
re.compile(NEO_VAR_NAME_LABEL_REGEX)


def get_connection():
    """
    Creates a connection object to the graph database using py2neo
    :return: py2neo GraphDatabaseService object
    """

    try:

        neo4j = config.get("neo4j")
    except errors.ConfigurationError:
        raise

    username = neo4j["username"]
    password = neo4j["password"]
    host = neo4j["host"]
    port = int(neo4j["port"])
    endpoint = neo4j["endpoint"]
    protocol = neo4j["protocol"]

    try:

        uri = "{0}://{1}:{2}/{3}".format(protocol, host, port, endpoint)

        db_conn = GraphDatabase(uri, username=username, password=password)

        neo4jrestclient.options.URI_REWRITES = {
            "http://0.0.0.0:7474/": "{0}://{1}:{2}/".format(protocol, host, port)
        }

    except exceptions.TransactionException as exc:
        raise exc
    else:
        return db_conn


def is_valid_label(label):
    """

    :param label:
    :return:
    """

    if re.match(NEO_VAR_NAME_LABEL_REGEX, label):
        return True
    else:
        return False


class Parameter(object):
    """

    """

    def __init__(self, key, value):

        self.key = key
        self.value = value

    def __repr__(self):

        return "Parameter({0}={1})".format(self.key, self.value)

    def __eq__(self, other):

        if isinstance(other, Parameter):
            return self.key == other.key and self.value == other.value
        else:
            return False

    def __ne__(self, other):

        return not self.__eq__(other)

    def __hash__(self):

        return hash(self.__repr__())


class Query(object):
    """

    """

    def __init__(self, statement, params):

        self.statement = statement
        self.params = params


class CypherQuery(object):

    def __init__(self, statement, commit=False):

        self.__statement = statement
        self.__commit = commit

    def __call__(self, f):

        def wrapped_f(*args, **kwargs):

            params = []

            for key in kwargs:
                params.append(Parameter(key, kwargs[key]))

            query = Query(self.__statement, params)

            r = run_query(query, self.__commit)

            return f(result=r, *args, **kwargs)

        return wrapped_f


def run_query(query, commit=False, timeout=None):
    """

    :param query:
    :param commit:
    :param timeout:
    :return:
    """

    graph = get_connection()
    tx = graph.transaction(for_query=True)

    q = query.statement
    p = {param.key: param.value for param in query.params}

    tx.append(q, params=p)

    try:

        result = tx.execute()[0]

    except exceptions.TransactionException as exc:
        Logger.error("Error initiating transaction:\n\t{0}".format(exc))
        raise exc

    if commit:
        tx.commit()

    return result


#todo: add timeout parameter,for py2neo
def run_batch_query(queries, commit, timeout=None):
    """

    :param queries:
    :param commit:
    :param timeout:
    :return:
    """

    graph = get_connection()
    tx = graph.transaction(for_query=True)

    for query in queries:
        statement = query.statement
        params = {param.key: param.value for param in query.params}

        tx.append(statement, params)

    try:

        results = tx.execute()

    except exceptions.TransactionException as exc:
        Logger.error("Error initiating transaction:\n\t{0}".format(exc))
        raise exc

    if commit:
        tx.commit()

    collection = []
    for result in results:

        records = []

        for record in result:

            records.append(record)

        collection.append(records)

    return collection
