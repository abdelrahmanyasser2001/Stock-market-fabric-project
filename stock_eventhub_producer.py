# pip install azure-eventhub azure-identity mimesis

import json
import time
import random
import logging
import sys
import hashlib
from datetime import datetime, timezone
from azure.eventhub import EventHubProducerClient, EventData
from azure.identity import DefaultAzureCredential
from mimesis import Finance

# === Azure Event Hub Configuration ===
EVENTHUB_NAMESPACE = 'stockevents-namespace.servicebus.windows.net'  # YOUR_EVENTHUB_NAMESPACE
EVENTHUB_NAME = 'stock-events'  # YOUR_EVENTHUB_NAME

# === Logging Setup ===
logging.basicConfig(
    format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    level=logging.INFO,
    handlers=[
        logging.FileHandler("stock_eventhub_producer.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger('stock_eventhub_producer')

# === Event Hub Producer Client ===
credential = DefaultAzureCredential()
producer = EventHubProducerClient(
    fully_qualified_namespace=EVENTHUB_NAMESPACE,
    eventhub_name=EVENTHUB_NAME,
    credential=credential
)

# === Generate Stock Symbols and Initial Prices ===
finance = Finance()
stock_symbols = [finance.stock_ticker() for _ in range(10)]
stock_prices = {symbol: random.uniform(100, 500) for symbol in stock_symbols}
previous_prices = stock_prices.copy() # Initialize previous prices

def generate_stock_event():
    """Simulates a stock market event with fluctuating prices and calculates percentage change."""
    stock = random.choice(stock_symbols)
    change_percent = random.uniform(-1, 1)  # ±1% change
    new_price = round(stock_prices[stock] * (1 + change_percent / 100), 2)

    previous_price = previous_prices.get(stock)
    price_change_percentage = 0.0
    if previous_price is not None and previous_price != 0:
        price_change_percentage = round(((new_price - previous_price) / previous_price) * 100, 2)

    # Update previous price for the next event
    previous_prices[stock] = new_price
    stock_prices[stock] = new_price

    event = {
        "stock_symbol": stock,
        "price": new_price,
        "volume": random.randint(100, 10000),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "price_change_percentage": price_change_percentage
    }

    # Generate a unique event ID using SHA256
    event["event_id"] = hashlib.sha256(json.dumps(event, sort_keys=True).encode('utf-8')).hexdigest()

    return event

# === Start Streaming ===
logger.info("Starting stock data producer for Azure Event Hub...")

try:
    while True:
        stock_event = generate_stock_event()
        message = json.dumps(stock_event)

        # Create and send batch
        event_data_batch = producer.create_batch()
        event_data_batch.add(EventData(message))
        producer.send_batch(event_data_batch)

        logger.info(f"Sent stock event to Azure Event Hub: {message}")
        time.sleep(0.2)

except KeyboardInterrupt:
    logger.info("Stock data producer stopped by user.")
except Exception as e:
    logger.critical(f"Unexpected error occurred: {e}")
finally:
    producer.close()
    logger.info("Azure Event Hub producer closed.")