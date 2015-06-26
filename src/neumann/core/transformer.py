from neumann.core import model
from neumann.core import constants
from neumann.core.db import neo4j


class CypherTransformer(object):

    @classmethod
    def session(cls, entity):

        queries = []

        template = 'MERGE (session :`{SESSION_LABEL}` :`{STORE_ID}` {{id: {{id}} }})'

        statements = [
            template.format(
                SESSION_LABEL=constants.LABEL_SESSION,
                STORE_ID=entity.tenant
            )
        ]

        params = [neo4j.Parameter('id', entity.id)]

        queries.append(neo4j.Query(statement=''.join(statements), params=params))

        return queries

    @classmethod
    def agent(cls, entity):

        queries = []

        template = 'MERGE (agent :`{AGENT_LABEL}` :`{STORE_ID}` {{id: {{id}} }})'

        statements = [
            template.format(
                AGENT_LABEL=constants.LABEL_AGENT,
                STORE_ID=entity.tenant
            )
        ]

        params = [neo4j.Parameter('id', entity.id)]

        queries.append(neo4j.Query(statement=''.join(statements), params=params))

        return queries

    @classmethod
    def item(cls, entity):

        queries = []

        template = 'MERGE (item :`{ITEM_LABEL}` :`{STORE_ID}` {{id: {{id}} }})'

        statements = [
            template.format(
                ITEM_LABEL=constants.LABEL_ITEM,
                STORE_ID=entity.tenant
            )
        ]

        params = [neo4j.Parameter('id', entity.id)]

        for k, v in entity.fields.items():

            params.append(neo4j.Parameter(k, v))
            statements.append(
                '\nSET item.{0} = {{ {0} }}'.format(
                    k
                )
            )

        queries.append(neo4j.Query(statement=''.join(statements), params=params))

        return queries

    @classmethod
    def user(cls, entity):

        queries = []

        template = 'MERGE (user :`{USER_LABEL}` :`{STORE_ID}` {{id: {{id}} }})'

        statements = [
            template.format(
                USER_LABEL=constants.LABEL_USER,
                STORE_ID=entity.tenant
            )
        ]

        params = [neo4j.Parameter('id', entity.id)]

        for k, v in entity.fields.items():

            params.append(neo4j.Parameter(k, v))
            statements.append(
                '\nSET user.{0} = {{ {0} }}'.format(
                    k
                )
            )

        queries.append(neo4j.Query(statement=''.join(statements), params=params))

        return queries

    @classmethod
    def action(cls, entity):

        queries = []

        # (session)-[r]-(user)clear
        if entity.user:

            template = 'MERGE (session :`{SESSION_LABEL}` :`{STORE_ID}` {{id: {{session_id}} }})' \
                       '\nMERGE (user :`{USER_LABEL}` :`{STORE_ID}` {{id: {{user_id}} }})' \
                       '\nMERGE (session)-[r :`{REL}`]->(user)'

            statements = [
                template.format(
                    SESSION_LABEL=constants.LABEL_SESSION,
                    USER_LABEL=constants.LABEL_USER,
                    STORE_ID=entity.tenant,
                    REL=constants.REL_SESSION_TO_USER
                )
            ]

            params = [neo4j.Parameter('user_id', entity.user),
                      neo4j.Parameter('session_id', entity.session)]

            queries.append(neo4j.Query(statement=''.join(statements), params=params))

        # (session)-[r]-(agent)
        if entity.agent:

            template = 'MERGE (session :`{SESSION_LABEL}` :`{STORE_ID}` {{id: {{session_id}} }})' \
                       '\nMERGE (agent :`{AGENT_LABEL}` :`{STORE_ID}` {{id: {{agent_id}} }})' \
                       '\nMERGE (session)-[r :`{REL}`]->(agent)'

            statements = [
                template.format(
                    SESSION_LABEL=constants.LABEL_SESSION,
                    AGENT_LABEL=constants.LABEL_AGENT,
                    STORE_ID=entity.tenant,
                    REL=constants.REL_SESSION_TO_AGENT
                )
            ]

            params = [neo4j.Parameter('agent_id', entity.agent),
                      neo4j.Parameter('session_id', entity.session)]

            queries.append(neo4j.Query(statement=''.join(statements), params=params))

        # Actions
        if entity.name == constants.REL_ACTION_TYPE_VIEW:

            template = 'MERGE (session :`{SESSION_LABEL}` :`{STORE_ID}` {{id: {{session_id}} }})' \
                       '\nMERGE (item :`{ITEM_LABEL}` :`{STORE_ID}` {{id: {{item_id}} }})' \
                       '\nMERGE (session)-[r :`{REL}`]->(item)'

            statements = [
                template.format(
                    SESSION_LABEL=constants.LABEL_SESSION,
                    ITEM_LABEL=constants.LABEL_ITEM,
                    STORE_ID=entity.tenant,
                    REL=constants.REL_ACTION_TYPE_VIEW
                )
            ]

            params = [
                neo4j.Parameter('item_id', entity.item),
                neo4j.Parameter('session_id', entity.session),
                neo4j.Parameter('datetime', entity.timestamp)
            ]

            for k in ('datetime',):

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

            for k, v in entity.fields.items():

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

                params.append(
                    neo4j.Parameter(k, v)
                )

            queries.append(neo4j.Query(statement=''.join(statements), params=params))

        elif entity.name == constants.REL_ACTION_TYPE_ADD_TO_CART:

            # (item)-[r]-(session)

            template = 'MERGE (session :`{SESSION_LABEL}` :`{STORE_ID}` {{id: {{session_id}} }})' \
                       '\nMERGE (item :`{ITEM_LABEL}` :`{STORE_ID}` {{id: {{item_id}} }})' \
                       '\nMERGE (session)-[r :`{REL}`]->(item)'

            statements = [
                template.format(
                    SESSION_LABEL=constants.LABEL_SESSION,
                    ITEM_LABEL=constants.LABEL_ITEM,
                    STORE_ID=entity.tenant,
                    REL=constants.REL_ACTION_TYPE_ADD_TO_CART
                )
            ]

            params = [
                neo4j.Parameter('item_id', entity.item),
                neo4j.Parameter('session_id', entity.session),
                neo4j.Parameter('datetime', entity.timestamp),
            ]

            for k in ('datetime',):

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

            for k, v in entity.fields.items():

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

                params.append(
                    neo4j.Parameter(k, v)
                )

            queries.append(neo4j.Query(statement=''.join(statements), params=params))

        elif entity.name == constants.REL_ACTION_TYPE_BUY:

            # (item)-[r]-(session)

            template = 'MERGE (session :`{SESSION_LABEL}` :`{STORE_ID}` {{id: {{session_id}} }})' \
                       '\nMERGE (item :`{ITEM_LABEL}` :`{STORE_ID}` {{id: {{item_id}} }})' \
                       '\nMERGE (session)-[r :`{REL}`]->(item)'

            statements = [
                template.format(
                    SESSION_LABEL=constants.LABEL_SESSION,
                    ITEM_LABEL=constants.LABEL_ITEM,
                    STORE_ID=entity.tenant,
                    REL=constants.REL_ACTION_TYPE_BUY
                )
            ]

            params = [
                neo4j.Parameter('item_id', entity.item),
                neo4j.Parameter('session_id', entity.session),
                neo4j.Parameter('datetime', entity.timestamp)
            ]

            for k in ('datetime',):

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

            for k, v in entity.fields.items():

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

                params.append(
                    neo4j.Parameter(k, v)
                )

            queries.append(neo4j.Query(statement=''.join(statements), params=params))

        elif entity.name == constants.REL_ACTION_TYPE_STARTED_CHECKOUT:

            template = 'MERGE (session :`{SESSION_LABEL}` :`{STORE_ID}` {{id: {{session_id}} }})' \
                       '\nMERGE (item :`{ITEM_LABEL}` :`{STORE_ID}` {{id: {{item_id}} }})' \
                       '\nMERGE (session)-[r :`{REL}`]->(item)'

            statements = [
                template.format(
                    SESSION_LABEL=constants.LABEL_SESSION,
                    ITEM_LABEL=constants.LABEL_ITEM,
                    STORE_ID=entity.tenant,
                    REL=constants.REL_ACTION_TYPE_STARTED_CHECKOUT
                )
            ]

            params = [
                neo4j.Parameter('item_id', entity.item),
                neo4j.Parameter('session_id', entity.session),
                neo4j.Parameter('datetime', entity.timestamp)
            ]

            for k in ('datetime',):

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

            for k, v in entity.fields.items():

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

                params.append(
                    neo4j.Parameter(k, v)
                )

            queries.append(neo4j.Query(''.join(statements), params))

        elif entity.name == constants.REL_ACTION_TYPE_SEARCH:

            template = 'MERGE (session :`{SESSION_LABEL}` :`{STORE_ID}` {{ id: {{session_id}} }})' \
                       '\nMERGE (search :`{SEARCH_LABEL}` :`{STORE_ID}` {{ keywords: {{keywords}} }})' \
                       '\nMERGE (session)-[r :`{REL}`]->(search)'

            statements = [
                template.format(
                    SEARCH_LABEL=constants.LABEL_SEARCH,
                    SESSION_LABEL=constants.LABEL_SESSION,
                    STORE_ID=entity.tenant,
                    REL=constants.REL_ACTION_TYPE_SEARCH
                )
            ]

            params = [
                neo4j.Parameter('session_id', entity.session),
                neo4j.Parameter('datetime', entity.timestamp)
            ]

            for k in ('datetime',):

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

            for k, v in entity.fields.items():

                statements.append(
                    '\nSET r.{0} = {{ {0} }}'.format(
                        k
                    )
                )

                params.append(
                    neo4j.Parameter(k, v)
                )

            queries.append(neo4j.Query(''.join(statements), params))

        elif entity.name in (constants.REL_ACTION_TYPE_CHECK_DELETE_ITEM,
                             constants.REL_ACTION_TYPE_DELETE_ITEM):

            template = 'MATCH (item :`{ITEM_LABEL}` :`{STORE_ID}` {{id: {{id}} }})' \
                       '\nOPTIONAL MATCH (item)-[r]-(x)' \
                       '\nDELETE r, item'

            statements = [
                template.format(
                    ITEM_LABEL=constants.LABEL_ITEM,
                    STORE_ID=entity.tenant
                )
            ]

            params = [neo4j.Parameter('id', entity.item)]

            queries.append(neo4j.Query(statement=''.join(statements), params=params))

        return queries

    @classmethod
    def transform(cls, entity):

        queries = []

        if isinstance(entity, model.Session):

            queries.extend(cls.session(entity))

        elif isinstance(entity, model.Agent):

            queries.extend(cls.agent(entity))

        elif isinstance(entity, model.Item):

            queries.extend(cls.item(entity))

        elif isinstance(entity, model.User):

            queries.extend(cls.user(entity))

        elif isinstance(entity, model.Action):

            queries.extend(cls.action(entity))

        return queries


