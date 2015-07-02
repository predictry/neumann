import os
import os.path
import json

import requests

from neumann.core.db import neo4j
from neumann.core import constants
from neumann.core import errors
from neumann.utils import config


class Neo4jRepository(object):

    # @classmethod
    # @neo4j.CypherQuery('MATCH (n :`{LABEL}`) RETURN DISTINCT LABELS(n) AS labels'.format(
    #     LABEL=store.LABEL_ITEM))
    # def get_active_tenants(cls, *args, **kwargs):
    #     '''
    #
    #     :param kwargs:
    #     :return:
    #     '''
    #
    #     tenants = []
    #
    #     for row in kwargs['result']:
    #         tenants.extend([x for x in row[0] if x != store.LABEL_ITEM])
    #
    #     return tenants

    @classmethod
    def get_item_count_for_tenant(cls, tenant):

        s = ['MATCH (n :`{LABEL}` :`{TENANT}`)',
             'RETURN COUNT (n) AS c']

        template = '\n'.join(s)

        statement = template.format(LABEL=constants.LABEL_ITEM,
                                    TENANT=tenant)

        query = {
            'query': statement,
            'params': {
            }
        }

        try:
            cfg = config.get('neo4j')
        except errors.ConfigurationError:
            raise

        username = cfg['username']
        password = cfg['password']
        host = cfg['host']
        port = int(cfg['port'])
        endpoint = cfg['endpoint']
        protocol = cfg['protocol']

        url = '{protocol}://{host}:{port}/{endpoint}/cypher/'.format(
            protocol=protocol,
            host=host,
            port=port,
            endpoint=endpoint
        )

        session = requests.Session()
        session.auth = (username, password)

        r = session.post(url=url, data=json.dumps(query), timeout=300)

        if r.status_code == 200:
            content = r.json()

            return content['data'][0][0]
        else:
            raise Exception(r.status_code)

    @classmethod
    def get_tenant_list_of_items_id(cls, tenant, skip=0, limit=10):

        statement = 'MATCH (n :`{LABEL}` :`{TENANT}`) RETURN n.id AS id SKIP {{skip}} LIMIT {{limit}}'.format(
            LABEL=constants.LABEL_ITEM, TENANT=tenant
        )

        params = [neo4j.Parameter('limit', limit), neo4j.Parameter('skip', skip)]

        query = neo4j.Query(statement, params)

        result = neo4j.run_query(query, timeout=300)

        return [row[0] for row in result]

    # @classmethod
    # def get_tenant_items_categories(cls, tenant):
    #
    #     statement = 'MATCH (n :`Item` :`{TENANT}`) ' \
    #                 'WHERE HAS (n.category) ' \
    #                 'RETURN DISTINCT n.category AS category, COUNT(n.category) AS n;'.format(TENANT=tenant)
    #
    #     query = neo4j.Query(statement, list())
    #
    #     r = neo4j.run_query(query, timeout=300)
    #
    #     categories = {row[0]: row[1] for row in r}
    #
    #     return categories

#     @classmethod
#     def get_tenant_items_from_category(cls, tenant, category, skip=0, limit=10):
#
#         statement = 'MATCH (n :`Item` :`{TENANT}`) ' \
#                     'WHERE HAS (n.category) AND n.category = {{category}}' \
#                     'RETURN n.id AS id ' \
#                     'SKIP {{skip}} ' \
#                     'LIMIT {{limit}};'.format(TENANT=tenant)
#
#         params = [neo4j.Parameter('skip', skip), neo4j.Parameter('limit', limit), neo4j.Parameter('category', category)]
#
#         query = neo4j.Query(statement, params)
#
#         r = neo4j.run_query(query, timeout=300)
#
#         items = [x[0] for x in r]
#
#         return items
#
    @classmethod
    def download_tenant_items_to_a_folder(cls, tenant, directory, skip=0, limit=10):

        params = [neo4j.Parameter('limit', limit), neo4j.Parameter('skip', skip)]
        statement = 'MATCH (n :`{LABEL}` :`{TENANT}`) RETURN n AS item SKIP {{skip}} LIMIT {{limit}}'.format(
            LABEL=constants.LABEL_ITEM, TENANT=tenant
        )

        query = neo4j.Query(statement, params)

        r = neo4j.run_query(query, commit=False, timeout=300)

        items = [x[0].properties for x in r]
        paths = list()

        for item in items:

            file_name = '.'.join([item['id'], 'json'])

            tmp_file = os.path.join(directory, file_name)

            paths.append(tmp_file)

            with open(tmp_file, 'w') as fp:
                json.dump(item, fp)

        n = len(items)
        del items[:]

        return paths, n
