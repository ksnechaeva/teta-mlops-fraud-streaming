from __future__ import annotations

import json
import os
import time
import uuid

import pandas as pd
import plotly.express as px
import psycopg
import streamlit as st
from kafka import KafkaProducer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TRANSACTIONS_TOPIC = os.getenv("TRANSACTIONS_TOPIC", "transactions")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://fraud:fraud@postgres:5432/fraud")
KAFKA_BATCH_ROWS = int(os.getenv("KAFKA_BATCH_ROWS", "1000"))

st.set_page_config(page_title="Fraud Streaming", page_icon="🛡️", layout="wide")
st.title("🛡️ Real-time fraud detection")


@st.cache_resource
def kafka_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP,
        value_serializer=lambda value: json.dumps(value, default=str).encode("utf-8"),
        acks="all",
        retries=10,
    )


def send_dataframe(frame: pd.DataFrame) -> int:
    producer = kafka_producer()
    progress = st.progress(0)
    status = st.empty()
    total = len(frame)
    for batch_start in range(0, total, KAFKA_BATCH_ROWS):
        batch = frame.iloc[batch_start : batch_start + KAFKA_BATCH_ROWS]
        for row in batch.where(pd.notna(batch), None).to_dict(orient="records"):
            transaction_id = str(uuid.uuid4())
            producer.send(
                TRANSACTIONS_TOPIC,
                key=transaction_id.encode(),
                value={"transaction_id": transaction_id, "data": row},
            )
        producer.flush(timeout=60)
        sent = min(batch_start + len(batch), total)
        progress.progress(sent / total)
        status.caption(f"Отправлено в Kafka: {sent:,} из {total:,}")
    return total


def query_dataframe(query: str) -> pd.DataFrame:
    with psycopg.connect(DATABASE_URL) as connection:
        return pd.read_sql_query(query, connection)


send_tab, results_tab = st.tabs(["Отправить транзакции", "Посмотреть результаты"])

with send_tab:
    st.write("Загрузите `test.csv`. Весь файл будет отправлен в Kafka пакетами; каждая строка станет отдельным JSON-сообщением.")
    uploaded = st.file_uploader("CSV с транзакциями", type="csv")
    if uploaded is not None:
        dataframe = pd.read_csv(uploaded)
        st.dataframe(dataframe.head(10), use_container_width=True)
        st.caption(f"Строк: {len(dataframe):,}")
        if dataframe.empty:
            st.warning("CSV не содержит строк с транзакциями.")
        elif st.button("Отправить весь файл в Kafka", type="primary"):
            with st.spinner("Публикация сообщений..."):
                sent = send_dataframe(dataframe)
            st.success(f"Отправлено транзакций: {sent:,}")

with results_tab:
    st.write("Данные обновляются по кнопке и читаются из Postgre, а не напрямую из Kafka.")
    if st.button("Посмотреть результаты", type="primary"):
        try:
            frauds = query_dataframe(
                """
                SELECT transaction_id, score, fraud_flag, scored_at
                FROM transaction_scores
                WHERE fraud_flag = 1
                ORDER BY scored_at DESC
                LIMIT 10
                """
            )
            latest = query_dataframe(
                """
                SELECT score
                FROM transaction_scores
                ORDER BY scored_at DESC
                LIMIT 100
                """
            )
            st.subheader("10 последних подозрительных транзакций")
            if frauds.empty:
                st.info("Транзакций с fraud_flag = 1 пока нет.")
            else:
                st.dataframe(frauds, use_container_width=True, hide_index=True)

            st.subheader("Распределение последних 100 скоров")
            if latest.empty:
                st.info("В базе пока нет результатов скоринга.")
            else:
                fixed_scale = st.toggle("Показывать полную шкалу вероятности 0–1", value=False)
                bin_count = min(20, max(3, round(len(latest) ** 0.5)))
                figure = px.histogram(
                    latest,
                    x="score",
                    nbins=bin_count,
                    range_x=[0, 1] if fixed_scale else None,
                )
                figure.update_layout(yaxis_title="Количество", xaxis_title="Fraud score")
                st.plotly_chart(figure, use_container_width=True)
                minimum, median, maximum = latest["score"].agg(["min", "median", "max"])
                st.caption(
                    f"Транзакций на графике: {len(latest)} · "
                    f"min: {minimum:.6f} · median: {median:.6f} · max: {maximum:.6f}"
                )
        except Exception as error:
            st.error(f"Не удалось получить результаты из Postgre: {error}")

st.caption(f"Kafka: {BOOTSTRAP} · {time.strftime('%Y-%m-%d %H:%M:%S')}")
