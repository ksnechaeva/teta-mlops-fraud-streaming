# Real-time fraud scoring with Kafka, PostgreSQL and Streamlit

CPU-only сервис потокового скоринга транзакций. Сервис принимает строки формата `test.csv`, применяет предобученную ML-модель, публикует результат в Kafka и сохраняет его в PostgreSQL.

## Архитектура

```text
test.csv / Streamlit
        │
        ▼
Kafka: transactions
        │
        ▼
scorer (preprocessing + SGDClassifier inference, CPU)
        │
        ▼
Kafka: scores {transaction_id, score, fraud_flag}
        │
        ▼
score-writer ──► PostgreSQL ──► Streamlit results UI
```

Сервисы:

- `kafka`, `zookeeper`, `kafka-init` — брокер и автоматическое создание топиков;
- `scorer` — consumer `transactions`, preprocessing, inference и producer `scores`;
- `score-writer` — отдельный consumer `scores`, выполняющий idempotent upsert в PostgreSQL;
- `postgres` — витрина `transaction_scores`;
- `interface` — загрузка CSV, отправка сообщений, 10 последних fraud-транзакций и гистограмма последних 100 скоров;
- `kafka-ui` — просмотр топиков и сообщений.

## Быстрый запуск

Требования: Docker 20.10+ и Docker Compose v2.

```bash
git clone <PUBLIC_GITHUB_REPOSITORY_URL>
cd fraud-streaming-service
docker compose up --build -d
```

Первый запуск скачивает базовые образы и может занять несколько минут. Проверка состояния:

```bash
docker compose ps
docker compose logs -f scorer score-writer
```

Откройте:

- Streamlit: http://localhost:8501
- Kafka UI: http://localhost:8080

Все прикладные контейнеры должны иметь состояние `running`; PostgreSQL и Kafka — `healthy`.

## Проверка через UI

1. Откройте http://localhost:8501.
2. Во вкладке «Отправить транзакции» загрузите `test.csv`.
3. Нажмите «Отправить весь файл в Kafka». Файл публикуется пакетами по 1000 строк с индикатором прогресса, при этом каждая транзакция остаётся отдельным Kafka-сообщением.
4. Подождите несколько секунд и перейдите во вкладку «Посмотреть результаты».
5. Нажмите одноимённую кнопку. UI покажет последние fraud-записи и гистограмму скоров.

Для быстрой проверки можно использовать фрагмент на 10–100 строк, но жёсткого ограничения размера файла в интерфейсе нет. На гистограмме отображаются строго последние 100 результатов (или все имеющиеся, если их меньше). По умолчанию ось автоматически масштабируется по данным; переключатель позволяет показать полную шкалу вероятности от 0 до 1.

## Проверка без UI

Установите локальные зависимости `pandas` и `kafka-python`, затем:

```bash
python scripts/send_sample.py --csv /path/to/test.csv --rows 10
```

Проверьте строки в PostgreSQL:

```bash
docker compose exec postgres psql -U fraud -d fraud -c \
  "SELECT transaction_id, score, fraud_flag, scored_at FROM transaction_scores ORDER BY scored_at DESC LIMIT 10;"
```

Проверьте сообщения `scores` в Kafka:

```bash
docker compose exec kafka kafka-console-consumer \
  --bootstrap-server kafka:9092 --topic scores --from-beginning --max-messages 10
```

Контракт выходного сообщения:

```json
{
  "transaction_id": "1f0c5cbb-5e5c-43c7-8aca-91332121f945",
  "score": 0.873214,
  "fraud_flag": 1
}
```

## ML pipeline

Контейнер выполняет только inference. Артефакт `scorer_service/model/fraud_model.joblib` уже обучен и включён в репозиторий.

`preprocessing.py`:

- проверяет обязательные поля `test.csv`;
- извлекает циклические признаки часа и дня недели;
- логарифмирует сумму и население;
- рассчитывает разности координат клиента и продавца;
- преобразует категориальные признаки через фиксированный feature hashing (`2^16` признаков).

`scoring.py` загружает CPU-модель `SGDClassifier(loss="log_loss")`, вычисляет вероятность класса fraud и применяет сохранённый порог. Один и тот же preprocessing используется при офлайн-обучении и онлайн-inference, поэтому training-serving skew отсутствует.

Офлайн-переобучение не является частью запуска сервиса. При необходимости артефакт можно пересобрать отдельно:

```bash
python training/train_model.py \
  --train-csv /path/to/train.csv \
  --output scorer_service/model/fraud_model.joblib
```

Скрипт читает train по частям и не загружает весь 145-МБ CSV в память. Порог автоматически выбирается на сохранённой выборке по F2, чтобы учитывать сильный дисбаланс fraud-класса.

## Надёжность

- Kafka offsets подтверждаются только после успешной публикации/записи в PostgreSQL.
- Producer скоринга использует idempotence.
- PostgreSQL использует `transaction_id` как primary key и `ON CONFLICT ... DO UPDATE`, поэтому повторная доставка не создаёт дубликаты.
- Compose содержит healthchecks и ожидает готовности Kafka/PostgreSQL.
- Сервисы запускаются непривилегированными пользователями.
- Обучение и inference ограничены CPU.

## Тесты

```bash
python -m pip install -r requirements-dev.txt
pytest -q
docker compose config -q
```

## Остановка и очистка

```bash
docker compose down
```

Удалить также PostgreSQL volume со всеми результатами:

```bash
docker compose down -v
```

## Структура

```text
.
├── docker-compose.yml
├── interface/
├── postgres/init.sql
├── score_writer/
├── scorer_service/
│   ├── app.py
│   ├── model/fraud_model.joblib
│   └── src/
│       ├── preprocessing.py
│       └── scoring.py
├── scripts/send_sample.py
├── tests/test_pipeline.py
└── training/train_model.py
```
