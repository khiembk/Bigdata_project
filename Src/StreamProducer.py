import json
import time
import yfinance as yf
from confluent_kafka import Producer
import socket
import argparse
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

def fetch_and_produce_data(ticker, interval=60):
    stock = yf.Ticker(ticker)
    while True:
        # Get the latest market data
        hist = stock.history(period="1d", interval="1m")  # Fetch 1-minute interval data for the past day
        hist.reset_index(inplace=True)
        
        # Get the current price from the most recent data point
        current_price = hist.iloc[-1]["Close"]
        for _, row in hist.iterrows():
            # Convert each row into a dictionary format
            data = {
                "Ticker": str(ticker),
                "Time": row["Datetime"].strftime("%H:%M"),
                "Date": row["Datetime"].strftime("%Y-%m-%d"),  # Use 'Datetime' for minute interval data
                "Open": row["Open"],
                "High": row["High"],
                "Low": row["Low"],
                "Close": row["Close"],
                "Volume": row["Volume"],
                "Current Price": current_price
            }
            message = json.dumps(data)
            producer.produce('stock-data', message.encode('utf-8'), callback=delivery_report)
        
        print("data: ", data)
        # Wait for a minute before fetching new data
        producer.flush()
        time.sleep(interval)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Stream stock data to Kafka.')
    parser.add_argument('--ticker', type=str, required= False, help='Stock ticker symbol',default= 'AAPL')
    args = parser.parse_args()
    ticker = args.ticker
    
    fetch_and_produce_data(ticker)