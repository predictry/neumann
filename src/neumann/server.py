import json

from webargs import Arg
from webargs.flaskparser import use_args
from flask import Flask, Response, jsonify

from neumann import services
from neumann.core import errors
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
    'date': Arg(str, required=True, location='json')
})
def recommend(args):

    try:

        date = io.parse_date(args['date'])
        tenant = args['tenant']
        task = services.RecommendService.compute(str(date.date()), tenant)

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


if not app.debug:

    from neumann.utils import config
    from neumann.utils.logger import Logger

    logging = config.get("logging")

    Logger.setup_logging(logging["logconfig"])

    # setup database indexing
    from neumann.utils.config import PROJECT_BASE
    from neumann.core.db.neo4j import BatchTransaction
    from neumann.core.db.neo4j import Query
    import os.path

    index_file = os.path.join(PROJECT_BASE, 'resources', 'db', 'schema.index')

    if os.path.exists(index_file):
        indexes = []

        with open(index_file, 'r') as fp:

            for line in fp:

                if line:
                    indexes.append(line)

        with BatchTransaction() as transaction:

            for index in indexes:
                transaction.append(Query(statement=index, params=[]))

            transaction.execute()


if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)
