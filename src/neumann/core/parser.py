import json
import gzip

from neumann.core import model


def parse_record(filepath):

    entries = []

    with gzip.open(filepath, 'r') as fp:

        for line in fp:

            payload = json.loads(line)

            entry = parse_entry(payload)

            if entry:
                entries.append(entry)


def parse_entry(payload):

    try:

        objecttype = payload['type']
        data = payload['data']

        if objecttype == 'metadata':
            pass
        elif objecttype == 'Session':

            session = model.Session(
                id=data['id'],
                tenant=data['tenant'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return session

        elif objecttype == 'Agent':

            agent = model.Agent(
                id=data['id'],
                tenant=data['tenant'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return agent

        elif objecttype == 'User':

            user = model.User(
                id=data['id'],
                tenant=data['tenant'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return user

        elif objecttype == 'Item':

            item = model.Item(
                id=data['id'],
                tenant=data['tenant'],
                timestamp=data['timestamp'],
                fields=data['fields']
            )

            return item

        elif objecttype == 'Action':

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
