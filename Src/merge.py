import json
import os
from confluent_kafka import Producer
import socket
from datetime import datetime

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

# Define the date range for the merge command
start_date = datetime.strptime("2023-10-19", "%Y-%m-%d")
end_date = datetime.strptime("2024-10-19", "%Y-%m-%d")

# Sample data (replace this with your actual data)
data = [
    {"command": "merge", "date": "2024-10-11"},
    {"command": "merge", "date": "2024-10-12"},
    {"command": "merge", "date": "2024-10-13"},
    {"command": "merge", "date": "2024-10-14"},
    {"command": "merge", "date": "2024-10-15"},
]

# Produce JSON message
for record in data:
    # Parse the date from the record
    record_date = datetime.strptime(record["date"], "%Y-%m-%d")

    # Check if the record date is within the specified range
    if start_date <= record_date <= end_date:
        # Generate the MERGE command for the record
        merge_command = {
            "command": record["command"],
            "date": record["date"],
        }

        # Convert the merge command to a JSON string
        message = json.dumps(merge_command)
        
        # Produce the message to Kafka
        producer.produce('merge-commands', message.encode('utf-8'), callback=delivery_report)

# Wait for all messages to be delivered
producer.flush()