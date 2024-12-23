import json
import os
from confluent_kafka import Producer
import socket
from datetime import datetime,timedelta

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
start_date = datetime.strptime("2024-8-19", "%Y-%m-%d")
end_date = datetime.strptime("2024-10-19", "%Y-%m-%d")

current_date = start_date
while current_date <= end_date:
    # Generate the MERGE command for the current date
    merge_command = {
        "command": "merge",
        "date": current_date.strftime("%Y-%m-%d")
    }

    # Convert the merge command to a JSON string
    message = json.dumps(merge_command)
    
    # Produce the message to Kafka
    producer.produce('merge-commands', message.encode('utf-8'), callback=delivery_report)

    # Move to the next day
    current_date += timedelta(days=1)

# Wait for all messages to be delivered
producer.flush()