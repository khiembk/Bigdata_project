import json
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

# Sample data that matches the json_schema structure
data = [
    {
        "Ticker": "AAPL",
        "Date": "2024-10-22",
        "Open": 174.55,
        "High": 176.2,
        "Low": 173.89,
        "Close": 175.98,
        "Volume": 68500000,
        "Dividends": 0.0,
        "Stock Splits": 0.0
    },
    {
        "Ticker": "MSFT",
        "Date": "2024-10-22",
        "Open": 320.14,
        "High": 322.45,
        "Low": 319.12,
        "Close": 321.78,
        "Volume": 34200000,
        "Dividends": 0.0,
        "Stock Splits": 0.0
    },
    {
        "Ticker": "MSFT",
        "Date": "2024-10-22",
        "Open": 320.14,
        "High": 322.45,
        "Low": 319.12,
        "Close": 321.78,
        "Volume": 34200000,
        "Dividends": 0.0,
        "Stock Splits": 0.0
    },
    {
        "Ticker": "MSFT",
        "Date": "2024-10-22",
        "Open": 320.14,
        "High": 322.45,
        "Low": 319.12,
        "Close": 321.78,
        "Volume": 34200000,
        "Dividends": 0.0,
        "Stock Splits": 0.0
    },
    {
        "Ticker": "MSFT",
        "Date": "2024-10-22",
        "Open": 320.14,
        "High": 322.45,
        "Low": 319.12,
        "Close": 321.78,
        "Volume": 34200000,
        "Dividends": 0.0,
        "Stock Splits": 0.0
    },
    {
        "Ticker": "MSFT",
        "Date": "2024-10-22",
        "Open": 320.14,
        "High": 322.45,
        "Low": 319.12,
        "Close": 321.78,
        "Volume": 34200000,
        "Dividends": 0.0,
        "Stock Splits": 0.0
    },
    {
        "Ticker": "MSFT",
        "Date": "2024-10-22",
        "Open": 320.14,
        "High": 322.45,
        "Low": 319.12,
        "Close": 321.78,
        "Volume": 34200000,
        "Dividends": 0.0,
        "Stock Splits": 0.0
    }
]

# Produce JSON message
for record in data:
    # Convert each record to a JSON string
    message = json.dumps(record)
    producer.produce('test-topic', message.encode('utf-8'), callback=delivery_report)

# Wait for all messages to be delivered
producer.flush()