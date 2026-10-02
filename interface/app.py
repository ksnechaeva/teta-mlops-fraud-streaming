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
MAX_UPLOAD_ROWS = int(os.getenv("MAX_UPLOAD_ROWS", "10000"))

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
    total = len(frame)
    for position, row in enumerate(frame.to_dict(orient="records"), start=1):
        transaction_id = str(uuid.uuid4())
        producer.send(
            TRANSACTIONS_TOPIC,
            key=transaction_id.encode(),
            value={"transaction_id": transaction_id, "data": row},
        )
        if position % 25 == 0 or position == total:
            progress.progress(position / total)
    producer.flush(timeout=30)
    return total


def query_dataframe(query: str) -> pd.DataFrame:
    with psycopg.connect(DATABASE_URL) as connection:
        return pd.read_sql_query(query, connection)


send_tab, results_tab = st.tabs(["Отправить транзакции", "Посмотреть результаты"])

with send_tab:
    st.write("Загрузите `test.csv` или его небольшой фрагмент. Каждая строка отправляется отдельным JSON-сообщением.")
    uploaded = st.file_uploader("CSV с транзакциями", type="csv")
    if uploaded is not None:
        dataframe = pd.read_csv(uploaded)
        st.dataframe(dataframe.head(10), use_container_width=True)
        st.caption(f"Строк: {len(dataframe):,}")
        if len(dataframe) > MAX_UPLOAD_ROWS:
            st.warning(f"За один запуск можно отправить не более {MAX_UPLOAD_ROWS:,} строк.")
        elif st.button("Отправить в Kafka", type="primary"):
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
                figure = px.histogram(latest, x="score", nbins=20, range_x=[0, 1])
                figure.update_layout(yaxis_title="Количество", xaxis_title="Fraud score")
                st.plotly_chart(figure, use_container_width=True)
                st.caption(f"Транзакций на графике: {len(latest)}")
        except Exception as error:
            st.error(f"Не удалось получить результаты из Postgre: {error}")

st.caption(f"Kafka: {BOOTSTRAP} · {time.strftime('%Y-%m-%d %H:%M:%S')}")
