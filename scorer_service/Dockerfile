FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 OMP_NUM_THREADS=1 OPENBLAS_NUM_THREADS=1
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app.py ./app.py
COPY src ./src
COPY model ./model
RUN useradd --create-home appuser && chown -R appuser:appuser /app
USER appuser
CMD ["python", "app.py"]
