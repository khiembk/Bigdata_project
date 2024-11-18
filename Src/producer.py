import json
import os
from confluent_kafka import Producer
import socket

# Define Kafka producer configurations
conf = {
    'bootstrap.servers': "localhost:9094",  # Adjust this to your Kafka broker
    'client.id': socket.gethostname()
}

# Create the Kafka Producer
producer = Producer(conf)

# Define a callback for delivery reports
def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))

with open(os.path.join(os.path.dirname(__file__), "AAPL_stock_data.json"), "r") as json_file:
    data = json.load(json_file)

# Produce JSON message
for record in data:
    # Convert each record to a JSON string
    message = json.dumps(record)
    producer.produce('stock-data', message.encode('utf-8'), callback=delivery_report)

# Wait for all messages to be delivered
producer.flush()