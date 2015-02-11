__author__ = 'guilherme'

import sqlalchemy.sql
import sqlalchemy


def get_engine(connection_string):

    engine = sqlalchemy.create_engine(connection_string)

    return engine


def select(engine, model):

    conn = engine.connect()
    s = sqlalchemy.sql.select([model])

    result = conn.execute(s)

    return result

