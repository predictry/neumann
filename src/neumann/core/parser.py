import json
import gzip

from neumann.core import model


def parse_record(filepath):

    with gzip.open(filepath, 'rb') as fp:

        for line in fp:

            payload = json.loads(line.decode('utf-8'))

            entry = parse_entry(payload)

            if entry:
                yield entry


def parse_entry(payload):

    try:

        objecttype = payload['type']

        if objecttype == 'metadata':
            pass
        elif objecttype == 'Session':

            data = payload['data']

            session = model.Session(
                id=data['id'],
                tenant=data['tenant'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return session

        elif objecttype == 'Agent':

            data = payload['data']

            agent = model.Agent(
                id=data['id'],
                tenant=data['tenant'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return agent

        elif objecttype == 'User':

            data = payload['data']

            user = model.User(
                id=data['id'],
                tenant=data['tenant'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return user

        elif objecttype == 'Item':

            data = payload['data']

            item = model.Item(
                id=data['id'],
                tenant=data['tenant'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return item

        elif objecttype == 'Action':

            data = payload['data']

            action = model.Action(
                name=data['name'],
                tenant=data['tenant'],
                user=data['user'],
                agent=data['agent'],
                session=data['session'],
                item=data['item'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return action

    except KeyError:
        raise

    return None
