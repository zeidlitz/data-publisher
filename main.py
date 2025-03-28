import yaml
import logging
import json
import redis

from prometheus_client import Gauge, start_http_server

CONFIG_PATH = "/etc/data-publisher/config.yaml"

def load_config(path):
    with open(path, "r") as file:
        return yaml.safe_load(file)

config = load_config(CONFIG_PATH)

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

CONSUMER_STREAM = config.get("consumer_stream", "data_analysis")
CONSUMER_GROUP = config.get("consumer_group ","data_publisher")
CONSUMER_NAME = config.get("consumer_name","publisher")
REDIS_HOST = config.get('redis_host', 'localhost')
REDIS_PORT = config.get("redis_port", 6379)
METRICS_HOST = config.get('metrics_host', 'localhost')
METRICS_PORT = config.get("metrics_port", 8000)

gauges = set()
sentiment_gauge = Gauge(
        f"sentiment_analysis",
        "sentiment for given category in source",
        ["category", "source", "subsource"]
        )

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

def create_redis_consumer_group():
    try:
        logging.info(f"Creating consumer group {CONSUMER_GROUP} for {CONSUMER_STREAM}")
        redis_client.xgroup_create(CONSUMER_STREAM, CONSUMER_GROUP, id='0', mkstream=True)
    except Exception as e:
        logging.info(f"Exception {e}")
        pass

def get_sentiment(label):
    if label == "POSITIVE":
        return 1
    if label == "NEGATIVE":
        return -1
    return 0

def update_metrics(data):
    logging.info("Publishing metrics")
    for entry in data:
        sentiment = get_sentiment(entry["sentiment"])
        for category in entry["category"]:
            source = entry["source"]
            subsource = entry["subsource"]
            labels = (category, source, subsource)
            if labels not in gauges:
                gauges.add(labels)
            sentiment_gauge.labels(category, source, subsource).inc(sentiment)
    logging.info("Publishing complete")

def consume_stream():
    while True:
        try:
            messages = redis_client.xreadgroup(CONSUMER_GROUP, CONSUMER_NAME, {CONSUMER_STREAM: '>'}, count=1, block=5000)
            logging.info(f"Consuming messages from {CONSUMER_STREAM}")
            if messages:
                logging.info(f"Message received!")
                for stream, message_list in messages:
                    for message_id, message in message_list:
                        redis_client.xack(CONSUMER_STREAM, CONSUMER_GROUP, message_id)
                        return json.loads(message['data'])
        except Exception as e:
            print(f"Error: {e}")

def main():
    create_redis_consumer_group()
    logging.info(f"Publishing metrics to {METRICS_HOST}:{METRICS_PORT}")
    start_http_server(METRICS_PORT)
    while True:
        data = consume_stream()
        update_metrics(data)

if __name__ == "__main__":
    main()
