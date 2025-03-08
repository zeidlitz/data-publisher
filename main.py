import json
import pdb
import logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
DATA_SOURCE = "data.json"

def main():
    with open(DATA_SOURCE, "r", encoding="utf-8") as f:
        input_data = json.load(f)

    for entry in input_data:
        pdb.set_trace()


if __name__ == "__main__":
    main()
