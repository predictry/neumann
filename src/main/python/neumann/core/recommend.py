import requests

from collections import Counter
from operator import itemgetter

from neumann.core import errors
from neumann.core import constants
from neumann.core.db import neo4j


# TODO : search paths from one session to another, regardless of nodes in between

def _rank_most_popular_items(data, key, collection=False, n=5):

    all_items = []
    item_store = {}

    for item in data:
        if collection:
            all_items.extend(item[key])
        else:
            all_items.append(item[key])

        item_store[item[key]] = item

    counter = Counter(all_items)

    items_ranking = []
    for item in counter.most_common(n):
        frequency = float("{0:.2f}".format(item[1]/float(len(data))))

        i = {"frequency": frequency}
        i.update(item_store[item[0]])
        items_ranking.append(i)

    return sorted(items_ranking, key=itemgetter("frequency"), reverse=True)


def _generate(tenant, rtype, item_id, filters=None, limit=None, fields=None):

    statements = []
    params = []

    if rtype in ["oivt", "oipt"]:

        # TODO: Replace User with Agent?

        action = lambda x: {
            "oivt": constants.REL_ACTION_TYPE_VIEW,
            "oipt": constants.REL_ACTION_TYPE_BUY,
        }[x]

        template = """
            MATCH (s :`{TENANT}` :`{SESSION_LABEL}`)-[:{ACTION}]->(i :`{TENANT}` :`{ITEM_LABEL}` {{id : {{item_id}}}})
            WITH s, i
            MATCH (s)-[:{ACTION}]->(x :`{ITEM_LABEL}` :`{TENANT}`)
            WHERE x <> i
            RETURN x.id AS item, COUNT(x) AS n
            ORDER BY n DESC
            LIMIT {{limit}}
            """

        statements.append(
            template.format(
                TENANT=tenant, SESSION_LABEL=constants.LABEL_SESSION,
                ITEM_LABEL=constants.LABEL_ITEM,
                ACTION=action(rtype)
            )
        )

        params.append(neo4j.Parameter("item_id", item_id))
        params.append(neo4j.Parameter("limit", limit if limit else 10))

    elif rtype in ["oiv", "oip"]:

        # TODO: Replace User with Agent?

        # MATCH (i :`tenant` :`Item` {id: "itemId"})<-[r1 :`BUY`|:`VIEW`]-(s1 :`tenant` :`Session`)-[:`BY`]->
        # (u :`tenant` :`User`)<-[:`BY`]-(s2 :`tenant` :`Session`)-[:`BUY`|:`VIEW`]->(x :`tenant` :`Item`)
        # WHERE i <> x AND s1 <> s2
        # WITH x
        # LIMIT 300
        # RETURN x.id AS item, COUNT(x) AS n
        # ORDER BY n DESC
        # LIMIT 10

        action = lambda x: {
            "oiv": constants.REL_ACTION_TYPE_VIEW,
            "oip": constants.REL_ACTION_TYPE_BUY
        }[x]

        template = """
            MATCH (u:`{TENANT}`:User)<-[:BY]-(:`{TENANT}`:Session)-[:{ACTION}]->(i:`{TENANT}`:Item {{id:{{item_id}}}})
            WITH u,i
            MATCH (u)<-[:BY]-(:`{TENANT}`:Session)-[:{ACTION}]->(x:`{TENANT}`:Item)
            WHERE x <> i
            RETURN x.id AS item, COUNT(x) AS n
            ORDER BY n DESC
            LIMIT {{limit}}
            """

        statements.append(
            template.format(
                TENANT=tenant, SESSION_LABEL=constants.LABEL_SESSION,
                ITEM_LABEL=constants.LABEL_ITEM, USER_LABEL=constants.LABEL_USER,
                ACTION=action(rtype)
            )
        )

        params.append(neo4j.Parameter("item_id", item_id))
        params.append(neo4j.Parameter("volume", 300))
        params.append(neo4j.Parameter("limit", 10))

    elif rtype in ["anon-oiv", "anon-oip"]:

        # TODO: Replace User with Agent?

        # MATCH (i :`tenant` :`Item` {id: "itemId"})<-[r1 :`BUY`|:`VIEW`]-(s1 :`tenant` :`Session`)-[:`FROM`]->
        # (u :`tenant` :`Agent`)<-[:`FROM`]-(s2 :`tenant` :`Session`)-[:`BUY`|:`VIEW`]->(x :`tenant` :`Item`)
        # WHERE i <> x AND s1 <> s2
        # WITH x
        # LIMIT 300
        # RETURN x.id AS item, COUNT(x) AS n
        # ORDER BY n DESC
        # LIMIT 10

        # action = lambda x: {
        #     "anon-oiv": constants.REL_ACTION_TYPE_VIEW,
        #     "anon-oip": constants.REL_ACTION_TYPE_BUY
        # }[x]

        template = """
            MATCH (i :`{TENANT}` :`{ITEM_LABEL}` {{id:{{item_id}}}})<-[r1 :`BUY`|:`VIEW`]-(s1 :`{TENANT}` :`{SESSION_LABEL}`)\
            -[:`FROM`]->(a :`{TENANT}` :`{AGENT_LABEL}`)<-[:`FROM`]-(s2 :`{TENANT}` :`{SESSION_LABEL}`)-[:`BUY`|:`VIEW`]\
            ->(x :`{TENANT}` :`{ITEM_LABEL}`)
            WHERE i <> x AND s1 <> s2
            WITH x
            LIMIT {{volume}}
            RETURN x.id AS item, COUNT(x) AS n
            ORDER BY n DESC
            LIMIT {{limit}}
            """

        statements.append(
            template.format(
                TENANT=tenant, SESSION_LABEL=constants.LABEL_SESSION,
                ITEM_LABEL=constants.LABEL_ITEM, AGENT_LABEL=constants.LABEL_AGENT
            )
        )

        params.append(neo4j.Parameter("item_id", item_id))
        params.append(neo4j.Parameter("volume", 300))
        params.append(neo4j.Parameter("limit", 10))

    elif rtype in ["duo"]:

        # MATCH (s :`tenant` :`Session`)-[:`BUY`|`VIEW`]->(i :`tenant` :`Item` {id : "tenantId"})
        # WITH s, i
        # MATCH (s)-[ :`BUY`|`VIEW`]->(x :`Item` :`tenant`)
        # WHERE x <> i
        # RETURN x.id AS item, COUNT(x) AS n
        # ORDER BY n DESC
        # LIMIT 10

        template = """
            MATCH (s :`{TENANT}` :`{SESSION_LABEL}`)-[:`BUY`|:`VIEW`]->(i :`{TENANT}` :`{ITEM_LABEL}` {{id : {{itemId}}}})
            WITH s, i
            MATCH (s)-[ :`BUY`|:`VIEW`]->(x :`{ITEM_LABEL}` :`{TENANT}`)
            WHERE x <> i
            RETURN x.id AS item, COUNT(x) AS n
            ORDER BY n DESC
            LIMIT {{limit}}
            """

        statements.append(
            template.format(
                TENANT=tenant, SESSION_LABEL=constants.LABEL_SESSION, ITEM_LABEL=constants.LABEL_ITEM
            )
        )

        params.append(neo4j.Parameter("itemId", item_id))
        params.append(neo4j.Parameter("limit", limit if limit else 10))

    else:

        raise errors.UnknownRecommendationOption("Recommendation option `{0}` isn't recognized".format(rtype))

    return neo4j.Query(''.join(statements), params)


class RecommendationProvider(object):

    @classmethod
    def compute(cls, tenant, rtype, item_id, filters=None, limit=None, fields=None):

        query = _generate(tenant, rtype, item_id, filters, limit, fields)

        output = neo4j.run_query(query)

        if rtype in ["oivt", "oipt", "duo", "oiv", "oip", "anon-oiv", "anon-oip"]:

            items = []
            count = sum([record[1] for record in output])

            for record in output:
                item = dict(id=record[0])
                item["frequency"] = float("{0:.2f}".format(record[1]/float(count)))
                items.append(item)

            result = items

        else:

            raise errors.UnknownRecommendationOption("Recommendation option `{0}` isn't recognized".format(rtype))

        return result


class BatchRecommendationProvider(object):

    @classmethod
    def compute(cls, tenant, rtype, items, filters=None, limit=None, fields=None):
        results = []
        if rtype == 'similiar':
            for item_id in items:
                r = requests.get('http://fisher.predictry.com:8090/fisher/items/{0}/related/{1}'.format(tenant, item_id))
                result = r.json()
                result_items = []
                for result_item in result['items']:
                    result_items.append({'frequency': 1.0, 'id': result_item});
                results.append(result_items)
        else:
            queries = []
            for item_id in items:
                query = _generate(tenant, rtype, item_id, filters, limit, fields)
                queries.append(query)
            bresults = neo4j.run_batch_query(queries)
            for output in bresults:
                if rtype in ["oivt", "oipt", "duo", "oiv", "oip", "anon-oiv", "anon-oip"]:
                    items = []
                    count = sum([record[1] for record in output])
                    for record in output:
                        item = dict(id=record[0])
                        item["frequency"] = float("{0:.2f}".format(record[1]/float(count)))
                        items.append(item)
                    results.append(items)
                else:
                    raise errors.UnknownRecommendationOption("Recommendation option `{0}` isn't recognized".format(rtype))
        return results
