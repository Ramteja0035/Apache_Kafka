from flask import Flask, render_template, request, jsonify
from kafka import KafkaProducer
import json

app = Flask(__name__)

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

@app.route('/')
def home():
    return "Hello World!"

@app.route('/form')
def form():
    return render_template('form.html')

@app.route('/submit', methods=['POST'])
def submit():
    data = request.get_json()

    # Send to Kafka
    producer.send('demo-topic', value=data)
    producer.flush()

    return jsonify({"status": "success", "message": "Data sent successfully"}), 200

if __name__ == '__main__':
    app.run(debug=True, port=8000)
