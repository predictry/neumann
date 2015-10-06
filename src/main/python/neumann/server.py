import json

from webargs import Arg
from webargs.flaskparser import use_args
from flask import Flask, Response, jsonify

from neumann import services
from neumann.utils import io

app = Flask(__name__)


@app.route('/', methods=['GET'])
def index():

    return Response('Go!', status=200, mimetype='text/plain')


@app.route("/services/import-record", methods=["POST"])
@use_args({
    'tenant': Arg(str, required=True, location='json'),
    'date': Arg(str, required=True, location='json'),
    'hour': Arg(int, required=True, validate=lambda x: 0 <= x <= 23, error='Invalid hour', location='json')
})
def harvest(args):

    try:

        timestamp = io.parse_timestamp(args['date'], str(args['hour']))
        tenant = args['tenant']
        task = services.RecordImportService.harvest(timestamp, tenant)

    except ValueError as exc:
        message = 'Invalid date: {0}. Format should be `%Y-%m-%d`'.format(args['date'])
        Logger.error(exc)
        return jsonify(dict(message=message)), 400

    return Response(json.dumps(task, cls=io.DateTimeEncoder), status=200, mimetype="application/json")


@app.route("/services/recommend", methods=["POST"])
@use_args({
    'tenant': Arg(str, required=True, location='json'),
    'date': Arg(str, required=True, location='json'),
    'algorithm': Arg(str, required=True, validate=lambda x: len(x) > 1, error='Invalid `string` for algorithm',
                     location='json')
})
def recommend(args):

    tenant = args['tenant']
    algorithm = args['algorithm']

    try:

        date = io.parse_date(args['date'])
        task = services.RecommendService.compute(str(date.date()), tenant, algorithm)

    except ValueError as exc:
        message = 'Invalid date: {0}. Format should be `YYYY-mm-dd`'.format(args['date'])
        Logger.error(exc)
        return jsonify(dict(message=message)), 400

    return Response(json.dumps(task, cls=io.DateTimeEncoder), status=200, mimetype="application/json")


@app.route("/services/trim-data", methods=["POST"])
@use_args({
    'tenant': Arg(str, required=True, location='json'),
    'date': Arg(str, required=True, location='json'),
    'startingDate': Arg(str, required=True, location='json'),
    'period': Arg(int, required=True, validate=lambda x: x >= 1, error='Invalid period', location='json')
})
def trimdata(args):

    try:

        date = io.parse_date(args['date'])
    except ValueError as exc:
        message = 'Invalid date: {0}. Format should be `YYYY-mm-dd`'.format(args['date'])
        Logger.error(exc)
        return jsonify(dict(message=message)), 400

    try:
        starting_date = io.parse_date(args['startingDate'])
    except ValueError as exc:
        message = 'Invalid date: {0}. Format should be `YYYY-mm-dd`'.format(args['date'])
        Logger.error(exc)
        return jsonify(dict(message=message)), 400

    tenant = args['tenant']
    period = args['period']

    task = services.DataTrimmingService.trim(
        date=str(date.date()),
        tenant=tenant,
        starting_date=starting_date.date(),
        period=period
    )

    return Response(json.dumps(task, cls=io.DateTimeEncoder), status=200, mimetype="application/json")


@app.errorhandler(400)
def handle_bad_request(err):
    data = getattr(err, 'data', None)
    if data:
        err_message = data['message']
    else:
        err_message = 'Invalid request'
    return jsonify({
        'message': err_message,
    }), 400


@app.errorhandler(500)
def handle_bad_request(err):
    data = getattr(err, 'data', None)
    if data:
        err_message = data['message']
    else:
        err_message = 'Internal Server Error'
    return jsonify({
        'message': err_message,
    }), 500

from neumann.utils.logger import Logger
from neumann.utils.config import LOGGING_CONFIG_FILE

Logger.setup_logging(LOGGING_CONFIG_FILE)

# setup database indexing
from neumann.core.db.neo4j import BatchTransaction
from neumann.core.db.neo4j import Query
import os.path

index_statements = [
    'CREATE INDEX ON :`Search`(keywords);',
    'CREATE INDEX ON :`User`(id);',
    'CREATE INDEX ON :`Session`(id);',
    'CREATE INDEX ON :`Session`(timestamp);',
    'CREATE INDEX ON :`Agent`(id);',
    'CREATE INDEX ON :`Item`(id);',
    'CREATE INDEX ON :`Item`(end_date);',
    'CREATE INDEX ON :`Item`(categories);']

with BatchTransaction() as transaction:
    for statement in index_statements:
        transaction.append(Query(statement=statement, params=[]))
    transaction.execute()

if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)
