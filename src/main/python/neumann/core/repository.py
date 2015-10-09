import os
import os.path
import json

import requests

from neumann.core.db import neo4j
from neumann.core import constants
from neumann.core import errors
from neumann.utils import config


class Neo4jRepository(object):

    @classmethod
    def delete_events_prior_to(cls, tenant, timestamp, limit=1000):
        # delete any actions tied to the session... (relationships)

        # transaction = neo4j.BatchTransaction()
        with neo4j.BatchTransaction() as transaction:

            s = [
                "MATCH (s :`{LABEL}` :`{TENANT}`)",
                "WHERE s.timestamp < {{timestamp}}",
                "WITH s",
                "LIMIT {{limit}}",
                "OPTIONAL MATCH (s)-[r]-(x)",
                "WITH s, r",
                "DELETE r, s",
                "RETURN COUNT(s) AS n"
            ]

            template = '\n'.join(s)

            statement = template.format(
                LABEL=constants.LABEL_SESSION,
                TENANT=tenant
            )

            params = [
                neo4j.Parameter('timestamp', timestamp),
                neo4j.Parameter('limit', limit)
            ]

            query = neo4j.Query(statement, params)

            transaction.append(query)
            # result = neo4j.run_query(query)
            # cc = result[0][0]

            # delete isolated nodes
            for label in [constants.LABEL_ITEM, constants.LABEL_AGENT, constants.LABEL_USER]:

                s = [
                    "MATCH (n :`{LABEL}` :`{TENANT}`)-[r]-()",
                    "WHERE r is NULL",
                    "WITH n",
                    "LIMIT {{limit}}",
                    "DELETE n"
                ]

                template = '\n'.join(s)

                statement = template.format(
                    LABEL=label,
                    TENANT=tenant
                )

                query = neo4j.Query(statement, [neo4j.Parameter('limit', limit)])

                # neo4j.run_query(query)
                transaction.append(query)

            rs = transaction.execute()

        return rs[0][0][0]

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

        statement = 'MATCH (n :`{LABEL}` :`{TENANT}`) RETURN n.id AS id ORDER BY n.id SKIP {{skip}} LIMIT {{limit}}'.format(
            LABEL=constants.LABEL_ITEM, TENANT=tenant
        )

        params = [neo4j.Parameter('limit', limit), neo4j.Parameter('skip', skip)]

        query = neo4j.Query(statement, params)

        result = neo4j.run_query(query, timeout=300)

        return [row[0] for row in result]

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
