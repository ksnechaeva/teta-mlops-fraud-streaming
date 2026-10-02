"""Consume raw Kafka transactions and publish model scores."""

from __future__ import annotations

import json
import logging
import os
import signal
import time

from confluent_kafka import Consumer, KafkaException, Producer

from src.scoring import FraudScorer

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("fraud-scorer")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
INPUT_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
OUTPUT_TOPIC = os.getenv("SCORES_TOPIC", "scores")


def delivery_report(error, message) -> None:
    if error:
        LOGGER.error("Kafka delivery failed: %s", error)


def score_payload(payload: dict, scorer: FraudScorer) -> dict:
    transaction_id = str(payload["transaction_id"])
    transaction = payload["data"]
    score, fraud_flag = scorer.predict(transaction)
    return {"transaction_id": transaction_id, "score": score, "fraud_flag": fraud_flag}


def main() -> None:
    scorer = FraudScorer()
    consumer = Consumer(
        {
            "bootstrap.servers": BOOTSTRAP,
            "group.id": "fraud-scorer-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
    running = True

    def stop(*_) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer.subscribe([INPUT_TOPIC])
    LOGGER.info("Scorer started: %s -> %s", INPUT_TOPIC, OUTPUT_TOPIC)
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                payload = json.loads(message.value())
                result = score_payload(payload, scorer)
                producer.produce(
                    OUTPUT_TOPIC,
                    key=result["transaction_id"],
                    value=json.dumps(result),
                    callback=delivery_report,
                )
                producer.flush(10)
                consumer.commit(message=message, asynchronous=False)
                LOGGER.info("Scored transaction=%s score=%.6f flag=%d", result["transaction_id"], result["score"], result["fraud_flag"])
            except Exception:
                LOGGER.exception("Message rejected; offset is not committed")
                time.sleep(1)
    finally:
        producer.flush(10)
        consumer.close()


if __name__ == "__main__":
    main()
