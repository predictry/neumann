__author__ = 'guilherme'

import re

from py2neo import Graph, Node, Relationship, rewrite
from py2neo.packages.httpstream import http
import py2neo.error
import py2neo.cypher
import py2neo.cypher.error.transaction


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

        py2neo.authenticate("{host}:{port}".format(host=host, port=port), username, password)

        uri = "{0}://{1}:{2}/{3}".format(protocol, host, port, endpoint)

        db_conn = Graph(uri)

        #neo4j bug

        rewrite(("http", "0.0.0.0", 7474), (protocol, host, port))

    except http.SocketError as err:
        raise err
    else:
        return db_conn


def is_node_exists(label, key, value):
    """
    Check if a node with a given label and (key, value) pair exists
    :param label: Label of node to check
    :param key: Property of node to check
    :param value: Value of property to check
    :return: True if at least one node matching the criteria is found. False otherwise.
    """

    if not is_valid_label(label):
        raise errors.InvalidLabelError("'{0}' is not a valid label".format(
            label))

    graph_db = get_connection()
    nodes = list(graph_db.find(label, property_key=key, property_value=value))

    if nodes:
        return True
    else:
        return False


def get_node(label, key, value):
    """
    Retrieves a node with a given label and (key, value) pair
    :param label: Label of node to retrieve
    :param key: Property to identify node
    :param value: Value of property to identify node
    :return: py2neo Node object
    """

    if not is_valid_label(label):
        raise errors.InvalidLabelError("'{0}' is not a valid label".format(
            label))

    graph_db = get_connection()
    n, = list(graph_db.find(label, property_key=key, property_value=value))

    return n


def get_relationship(start_node, rel_type, end_node=None, limit=1):
    """
    Retrieves a relationship between
    :param start_node: Start node in relationship. None for any
    :param rel_type: Type of relationship
    :param end_node: End node in relationship. None for any.
    :param limit:
    :return:
    """

    if not is_valid_label(rel_type):
        raise errors.InvalidRelationshipTypeError("'{0}' is not a valid relationship type".format(
            rel_type))

    graph_db = get_connection()
    relationship, =  list(graph_db.match(start_node=start_node, rel_type=rel_type, end_node=end_node, limit=limit))

    return relationship


def get_relationships(start_node, rel_type, end_node=None):
    """

    :param start_node: Start node on relationships None for any.
    :param rel_type:
    :param end_node: End node in relationships. None for any.
    :return:
    """

    if not is_valid_label(rel_type):
        raise errors.InvalidRelationshipTypeError("'{0}' is not a valid relationship type".format(
            rel_type))

    graph_db = get_connection()
    relationships = list(graph_db.match(start_node=start_node, rel_type=rel_type, end_node=end_node))

    return relationships


def traverse_path(start_node, rel_type):

    nodes = []

    rels = get_relationships(start_node, rel_type, None)

    x = [rel.end_node for rel in rels]

    nodes.extend(x)

    for node in x:
        nodes.extend(traverse_path(node, rel_type))

    return set(nodes)


def is_relationship_exists(start_node, rel_type, end_node, limit=1):
    """

    :param start_node:
    :param rel_type:
    :param end_node:
    :param limit:
    :return:
    """

    if not is_valid_label(rel_type):
        raise errors.InvalidRelationshipTypeError("'{0}' is not a valid relationship type".format(
            rel_type))

    graph_db = get_connection()

    relationships = list(graph_db.match(start_node=start_node, rel_type=rel_type, end_node=end_node, limit=limit))

    if relationships:
        return True
    else:
        return False


def create_node(properties, labels):
    """

    :param properties:
    :param labels:
    :return:
    """

    for label in labels:
        if not is_valid_label(label):
            raise errors.InvalidLabelError("'{0}' is not a valid label".format(
                label))

    graph_db = get_connection()

    n, = graph_db.create(Node.cast(properties, labels))

    return n


def create_relationship(start_node, rel_type, end_node, properties=None):
    """

    :param start_node:
    :param rel_type:
    :param end_node:
    :param properties:
    :return:
    """

    if not is_valid_label(rel_type):
        raise errors.InvalidRelationshipTypeError("'{0}' is not a valid relationship type".format(
            rel_type))

    graph_db = get_connection()

    r, = graph_db.create(Relationship.cast(start_node, rel_type, end_node, properties if properties else {}))

    return r


def delete_node(n):
    """

    :param n:
    :return:
    """

    graph_db = get_connection()

    relationships = n.match(rel_type=None, other_node=None, limit=None)

    for r in relationships:
        graph_db.delete(r)

    graph_db.delete(n)

    return not n.exists


def delete_relationship(r):
    """

    :param r:
    :return:
    """

    graph_db = get_connection()

    graph_db.delete(r)

    return not r.exists


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


#todo: add timeout parameter,for py2neo
def run_query(query, commit=False, timeout=None):
    """

    :param query:
    :param commit:
    :param timeout:
    :return:
    """

    if timeout:
        http.socket_timeout = timeout

    try:
        graph = get_connection()
        tx = graph.cypher.begin()

    except http.SocketError as err:

        Logger.error("Error in executing query:\n\t{0}".format(err))
        raise err

    #todo: catch other execptions (outofmemory, wrong syntax)

    q = query.statement
    p = {param.key: param.value for param in query.params}

    tx.append(q, p)
    result = tx.process()[0]

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

    if timeout:
        http.socket_timeout = timeout

    graph = get_connection()
    tx = graph.cypher.begin()

    try:

        for query in queries:
            statement = query.statement
            params = {param.key: param.value for param in query.params}

            tx.append(statement, params)

        results = tx.process()
        tx.commit()

    except http.SocketError:
        tx.rollback()
        raise
    except py2neo.cypher.error.transaction.DeadlockDetected:
        tx.rollback()
        raise
    else:

        collection = []
        for result in results:

            records = []

            for record in result:

                records.append(record)

            collection.append(records)

        return collection
