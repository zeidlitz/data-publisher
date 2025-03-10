import time
import logging
import json
from prometheus_client import Gauge, start_http_server

gauges = {}

DATA_SOURCE = "data.json"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_sentiment(label):
    if label == "POSITIVE":
        return 1
    if label == "NEGATIVE":
        return -1
    return 0

def get_valid_prometheous_gauge_name(text):
    return text.replace("%", "percent")

def update_metrics(data):
    for entry in data:
        sentiment = get_sentiment(entry["sentiment"])
        for category in entry["category"]:
            labels = (category, entry["source"], entry["subsource"])
            if labels not in gauges:
                c = get_valid_prometheous_gauge_name(category)
                gauges[labels] = Gauge(
                        f"sentiment_{c}",
                        "sentiment for given category in source",
                        ["category", "source", "subsource"]
                        )
            gauges[labels].labels(*labels).inc(sentiment)

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
