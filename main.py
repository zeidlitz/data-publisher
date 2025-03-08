import time
import logging
import json
from prometheus_client import Gauge, start_http_server

metrics = {}

DATA_SOURCE = "data.json"
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

def get_label_value(label):
    if label == "POSITIVE":
        return 1
    if label == "NEGATIVE":
        return -1
    return 0

def update_metrics(data):
    for entry in data:
        value = get_label_value(entry["label"])
        for category in entry["category"]:
            if category not in metrics:
                try:
                    m = Gauge(f"{category.replace(' ', '_').lower()}", f"Sentiment for {category}")
                    metrics[category] = m
                except ValueError:
                    continue
            metrics[category].inc(value)

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
