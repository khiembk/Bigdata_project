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

# Produce message
for i in range(10):
    message = f"Message {i} from Spark"
    producer.produce('test-topic', message.encode('utf-8'), callback=delivery_report)

# Wait for all messages to be delivered
producer.flush()