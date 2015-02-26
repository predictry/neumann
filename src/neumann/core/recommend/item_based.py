__author__ = 'guilherme'

from collections import Counter
from operator import itemgetter

from neumann.core import errors
from neumann.core.model import store
from neumann.core.db import neo4j


def __rank_most_popular_items(data, key, collection=False, n=5):

    all_items = list()
    item_store = dict()

    for item in data:
        if collection:
            all_items.extend(item.properties[key])
        else:
            all_items.append(item.properties[key])

        item_store[item.properties[key]] = item.properties

    counter = Counter(all_items)

    items_ranking = []
    for item in counter.most_common(n):
        frequency = float("{0:.2f}".format(item[1]/float(len(data))))

        i = {"frequency": frequency}
        i.update(item_store[item[0]])
        items_ranking.append(i)

    return sorted(items_ranking, key=itemgetter("frequency"), reverse=True)


def __generate(tenant, rtype, item_id, filters=None, limit=None, fields=None):

    q = list()
    params = list()

    #other items viewed/purchased together
    if rtype in ["oivt", "oipt"]:

        #todo: make this a global lambda
        action = lambda x: {
            "oivt": store.REL_ACTION_TYPE_VIEW,
            "oipt": store.REL_ACTION_TYPE_BUY,
        }[x]

        template = """
            MATCH (s :`{TENANT}` :`{SESSION_LABEL}`)-[:`{REL}`]->(i :`{TENANT}` :`{ITEM_LABEL}` {{id : {{item_id}}}})
            WITH s, i
            MATCH (s)-[ :`{REL}`]->(x :`{ITEM_LABEL}` :`{TENANT}`)
            WHERE x <> i
            RETURN x AS item, COUNT(x) AS n
            ORDER BY n DESC
            LIMIT {{limit}}
            """

        q.append(template.format(TENANT=tenant, SESSION_LABEL=store.LABEL_SESSION,
                                 ITEM_LABEL=store.LABEL_ITEM, REL=action(rtype)))

        params.append(neo4j.Parameter("item_id", item_id))
        params.append(neo4j.Parameter("limit", limit if limit else 10))

    elif rtype in ["oiv", "oip"]:

        #todo: user vs agent as default?
        #this query looks for items purchased/viewed by people that purchased/viewed this item

        action = lambda x: {
            "oiv": store.REL_ACTION_TYPE_VIEW,
            "oip": store.REL_ACTION_TYPE_BUY
        }[x]

        template = """
            MATCH (i :`{TENANT}` :`{ITEM_LABEL}` {{id:{{item_id}}}})<-[r1 :`{REL}`]-(s1 :`{TENANT}` :`{SESSION_LABEL}`)\
            -[:`BY`]->(u :`{TENANT}` :`{USER_LABEL}`)<-[:`BY`]-(s2 :`{TENANT}` :`{SESSION_LABEL}`)-[:`{REL}`]\
            ->(x :`{TENANT}` :`{ITEM_LABEL}`)
            WHERE i <> x AND s1 <> s2
            RETURN x
            LIMIT {{limit}}
            """

        q.append(template.format(TENANT=tenant, SESSION_LABEL=store.LABEL_SESSION,
                                 ITEM_LABEL=store.LABEL_ITEM, USER_LABEL=store.LABEL_USER,
                                 REL=action(rtype)))

        params.append(neo4j.Parameter("item_id", item_id))
        params.append(neo4j.Parameter("limit", 300))

    elif rtype in ["anon-oiv", "anon-oip"]:

        action = lambda x: {
            "anon-oiv": store.REL_ACTION_TYPE_VIEW,
            "anon-oip": store.REL_ACTION_TYPE_BUY
        }[x]

        template = """
            MATCH (i :`{TENANT}` :`{ITEM_LABEL}` {{id:{{item_id}}}})<-[r1 :`{REL}`]-(s1 :`{TENANT}` :`{SESSION_LABEL}`)\
            -[:`FROM`]->(a :`{TENANT}` :`{AGENT_LABEL}`)<-[:`FROM`]-(s2 :`{TENANT}` :`{SESSION_LABEL}`)-[:`{REL}`]\
            ->(x :`{TENANT}` :`{ITEM_LABEL}`)
            WHERE i <> x AND s1 <> s2
            RETURN x
            LIMIT {{limit}}
            """

        q.append(template.format(TENANT=tenant, SESSION_LABEL=store.LABEL_SESSION,
                                 ITEM_LABEL=store.LABEL_ITEM, AGENT_LABEL=store.LABEL_AGENT,
                                 REL=action(rtype)))

        params.append(neo4j.Parameter("item_id", item_id))
        params.append(neo4j.Parameter("limit", 300))

    elif rtype in ["trv", "trp", "trac"]:

        action = lambda x: {
            "trv": store.REL_ACTION_TYPE_VIEW,
            "trp": store.REL_ACTION_TYPE_BUY,
            "trac": store.REL_ACTION_TYPE_ADD_TO_CART,
        }[x]

        template = """
            MATCH (s :`{TENANT}` :`{SESSION_LABEL}`)-[r :`{REL}`]->(x :`{TENANT}` :`{ITEM_LABEL}`)
            WITH s, r, x
            ORDER BY r.datetime DESC
            LIMIT {{n_actions}}
            RETURN x AS item, COUNT(x) AS n
            ORDER BY n DESC
            LIMIT {{limit}}
            """

        q.append(template.format(TENANT=tenant, SESSION_LABEL=store.LABEL_SESSION,
                                 ITEM_LABEL=store.LABEL_ITEM, REL=action(rtype)))

        params.append(neo4j.Parameter("n_actions", 1000))
        params.append(neo4j.Parameter("limit", 10))

    else:

        raise errors.UnknownRecommendationOption("Recommendation option `{0}` isn't recognized".format(rtype))

    return neo4j.Query(''.join(q), params)


def compute_recommendation(tenant, rtype, item_id, filters=None, limit=None, fields=None):

    query = __generate(tenant, rtype, item_id, filters, limit, fields)

    output = neo4j.run_query(query, commit=False)

    if rtype in ["oivt", "oipt", "trv", "trp", "trac"]:

        items = []
        item_count = 0

        for record in output:
            item_count += record[1]

        for record in output:
            item = record[0].properties
            item["frequency"] = float("{0:.2f}".format(record[1]/float(item_count)))
            items.append(item)

        result = items

    elif rtype in ["oiv", "oip", "anon-oiv", "anon-oip"]:

        collections = []
        for record in output:
            collections.append(record[0])

        limit = limit if limit else 10

        most_popular_items = __rank_most_popular_items(collections, key="id", n=limit)

        result = most_popular_items

    else:

        raise errors.UnknownRecommendationOption("Recommendation option `{0}` isn't recognized".format(rtype))

    return result