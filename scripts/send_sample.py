from __future__ import annotations

import argparse
import json
import uuid

import pandas as pd
from kafka import KafkaProducer


def main() -> None:
    parser = argparse.ArgumentParser(description="Publish a small test.csv sample")
    parser.add_argument("--csv", required=True)
    parser.add_argument("--rows", type=int, default=10)
    parser.add_argument("--bootstrap", default="localhost:9095")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=args.bootstrap,
        value_serializer=lambda value: json.dumps(value, default=str).encode(),
        acks="all",
    )
    frame = pd.read_csv(args.csv, nrows=args.rows)
    for row in frame.to_dict(orient="records"):
        transaction_id = str(uuid.uuid4())
        producer.send(
            "transactions",
            key=transaction_id.encode(),
            value={"transaction_id": transaction_id, "data": row},
        )
    producer.flush()
    print(f"Published {len(frame)} transactions")


if __name__ == "__main__":
    main()
