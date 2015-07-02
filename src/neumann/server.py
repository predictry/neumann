import json

from webargs import Arg
from webargs.flaskparser import use_args
from flask import Flask, Response, jsonify


app = Flask(__name__)

@app.route('/', methods=['GET'])
def index():

    return Response('Go!', status=200, mimetype='text/plain')


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


@app.route("/services/", methods=["POST"])
@use_args({
    'name': Arg(str, required=True, location='json'),
    'tenant': Arg(str, required=True, location='json'),
    'output': Arg({
        'store': Arg(str, required=True),
        'fields': Arg({
            'bucket': Arg(str, required=False),
            'path': Arg(str, required=False)
        }, required=True),
    }, required=True, location='json')
})
def service(args):

    data = {
        "App": "{0}".format("Neumann")
    }

    return Response(json.dumps(data), status=200, mimetype="application/json")


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


if __name__ == "__main__":
    app.run(debug=True, use_reloader=True)
