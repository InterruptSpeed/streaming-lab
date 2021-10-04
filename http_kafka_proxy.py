from flask import request, jsonify
import flask

app = flask.Flask(__name__)

from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})

@app.route('/topic', methods=['POST'])
def topic():
    d = request.get_json()
    url = d.get('url')
    if url is None:
        return 'failure', 400
    p.produce('inbox.urls', url.encode('utf-8'))
    return 'success', 200

@app.route('/', methods=['GET'])
def home():
    return 'success', 200

app.run()

