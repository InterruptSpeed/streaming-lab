from flask import request
import flask

app = flask.Flask(__name__)

from confluent_kafka import Producer

p = Producer({'bootstrap.servers': 'localhost:9092'})

@app.route('/topic', methods=['POST'])
def topic():
    url = request.values.get('url')
    p.produce('inbox.urls', url.encode('utf-8'))
    return '', 200

@app.route('/', methods=['GET'])
def home():
    return '', 200

app.run()

