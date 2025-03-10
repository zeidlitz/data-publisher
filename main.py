import time
import logging
import json
from prometheus_client import Gauge, start_http_server

gauges = set()
sentiment_gauge = Gauge(
        f"sentiment_analysis",
        "sentiment for given category in source",
        ["category", "source", "subsource"]
        )

DATA_SOURCE = "data.json"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_sentiment(label):
    if label == "POSITIVE":
        return 1
    if label == "NEGATIVE":
        return -1
    return 0

def update_metrics(data):
    for entry in data:
        sentiment = get_sentiment(entry["sentiment"])
        for category in entry["category"]:
            source = entry["source"]
            subsource = entry["subsource"]
            labels = (category, source, subsource)
            if labels not in gauges:
                gauges.add(labels)
            sentiment_gauge.labels(category, source, subsource).inc(sentiment)

def load_data():
    with open(DATA_SOURCE, "r", encoding="utf-8") as f:
        input_data = json.load(f)
    return input_data

if __name__ == "__main__":
    start_http_server(8000)
    data = load_data()
    while True:
        logging.info("update_metrics")
        update_metrics(data)
        time.sleep(120)
