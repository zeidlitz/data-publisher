import yaml
import sys
import logging
import json
import redis

from prometheus_client import Gauge, Counter, start_http_server
from importlib.metadata import version, PackageNotFoundError


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")


sentiment_gauge = Gauge(
        f"sentiment_analysis",
        "Sentiment analysis and extracted categories from a source",
        ["category", "source", "subsource"]
        )

trends_counter = Counter(
        f"trends",
        "Counts trending topics",
        ["category", "source", "posted_in"]
        )

class ParseException(Exception):
    pass

def parse_args():
    args = sys.argv
    nrArgs = len(args)
    if nrArgs == 1:
        return "config.yaml"
    if nrArgs > 2:
        raise ParseException(
            f"too many input arguments, recieved {nrArgs} need exactlly one. Recieved: {args[1:]}"
        )
    return args[1]

def get_version():
    __version__ = "unknown"
    try:
        __version__ = version("data-extraction")
    except PackageNotFoundError:
        logging.warning("could not read pacakge version, ensure project is installed properly")
    return __version__

def load_config(path):
    with open(path, "r") as file:
        return yaml.safe_load(file)

def create_redis_consumer_group(redis_client, consumer_group, consumer_stream, _id, mkstream):
    try:
        logging.info(f"creating consumer group {consumer_group} for {consumer_stream}")
        redis_client.xgroup_create(consumer_stream, consumer_group, id=_id, mkstream=mkstream)
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
    update_sentiment_gauge(data)
    update_trends_gauge(data)

def update_sentiment_gauge(data):
    logging.info("Updating sentiment_analysis gauge")
    for entry in data:
        sentiment = get_sentiment(entry["sentiment"])
        for category in entry["category"]:
            source = entry["source"]
            subsource = entry["subsource"]
            sentiment_gauge.labels(category, source, subsource).inc(sentiment)

def update_trends_gauge(data):
    logging.info("Updating trends gauge")
    for entry in data:
        source = entry["source"]
        subsource = entry["subsource"]
        posted_in = entry["posted_in"]
        trends_counter.labels(source, subsource, posted_in).inc()

def consume_stream(redis_client, consumer_group, consumer_name, consumer_stream):
    while True:
        try:
            messages = redis_client.xreadgroup(consumer_group, consumer_name, {consumer_stream: '>'}, count=1, block=5000)
            logging.info(f"consuming messages from {consumer_stream}")
            if messages:
                logging.info(f"Message received!")
                for _, message_list in messages:
                    for message_id, message in message_list:
                        redis_client.xack(consumer_stream, consumer_group, message_id)
                        return json.loads(message['data'])
        except Exception as e:
            print(f"Error: {e}")

def main():
    __version__ = get_version()
    logging.info(f"Running version {__version__}")
    config_path = parse_args()
    config = load_config(config_path)
    consumer_stream = config.get("consumer_stream", "data_analysis")
    consumer_group = config.get("consumer_group ","data_publisher")
    consumer_name = config.get("consumer_name","publisher")
    redis_host = config.get('redis_host', 'localhost')
    redis_port = config.get("redis_port", 6379)
    metrics_host = config.get('metrics_host', 'localhost')
    metrics_port = config.get("metrics_port", 8000)
    redis_client = redis.Redis(host=redis_host, port=redis_port, decode_responses=True)
    create_redis_consumer_group(redis_client, consumer_group, consumer_stream, 0, True)
    logging.info(f"publishing metrics to {metrics_host}:{metrics_port}")
    start_http_server(metrics_port)
    while True:
        data = consume_stream(redis_client, consumer_group, consumer_name, consumer_stream)
        update_metrics(data)

if __name__ == "__main__":
    main()
