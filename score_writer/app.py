"""Persist Kafka score messages into Postgre with idempotent upserts."""

from __future__ import annotations

import json
import logging
import os
import signal
import time

import psycopg
from confluent_kafka import Consumer, KafkaException

logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"), format="%(asctime)s %(levelname)s %(message)s")
LOGGER = logging.getLogger("score-writer")


def connect_with_retry() -> psycopg.Connection:
    dsn = os.getenv("DATABASE_URL", "postgresql://fraud:fraud@postgres:5432/fraud")
    while True:
        try:
            return psycopg.connect(dsn, autocommit=True)
        except psycopg.OperationalError:
            LOGGER.warning("PostgreSQL is not ready; retrying")
            time.sleep(2)


def validate_score(payload: dict) -> tuple[str, float, int]:
    transaction_id = str(payload["transaction_id"])
    score = float(payload["score"])
    fraud_flag = int(payload["fraud_flag"])
    if not 0.0 <= score <= 1.0:
        raise ValueError("score must be in [0, 1]")
    if fraud_flag not in (0, 1):
        raise ValueError("fraud_flag must be 0 or 1")
    return transaction_id, score, fraud_flag


def main() -> None:
    connection = connect_with_retry()
    consumer = Consumer(
        {
            "bootstrap.servers": os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            "group.id": "postgres-score-writer-v1",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([os.getenv("SCORES_TOPIC", "scores")])
    running = True

    def stop(*_) -> None:
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    LOGGER.info("Score writer started")
    try:
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            try:
                values = validate_score(json.loads(message.value()))
                with connection.cursor() as cursor:
                    cursor.execute(
                        """
                        INSERT INTO transaction_scores (transaction_id, score, fraud_flag)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (transaction_id) DO UPDATE
                        SET score = EXCLUDED.score,
                            fraud_flag = EXCLUDED.fraud_flag,
                            scored_at = NOW()
                        """,
                        values,
                    )
                consumer.commit(message=message, asynchronous=False)
            except Exception:
                LOGGER.exception("Could not persist score; offset is not committed")
                time.sleep(1)
    finally:
        consumer.close()
        connection.close()


if __name__ == "__main__":
    main()
