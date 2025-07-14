FROM python:3.10-slim

WORKDIR /app

RUN apt-get update && \
    apt-get install -y curl unzip wget gnupg lsb-release && \
    wget https://github.com/duckdb/duckdb/releases/download/v0.9.2/duckdb_cli-linux-amd64.zip && \
    unzip duckdb_cli-linux-amd64.zip && \
    mv duckdb /usr/local/bin/ && \
    chmod +x /usr/local/bin/duckdb && \
    rm duckdb_cli-linux-amd64.zip && \
    apt-get clean

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

ENV KAGGLE_CONFIG_DIR=/app
ENV GOOGLE_APPLICATION_CREDENTIALS=/app/key.json

COPY ingest_sentiment140.py .
COPY kaggle.json /app/kaggle.json
COPY key.json /app/key.json
COPY .env /app/.env

ENTRYPOINT ["python", "ingest_sentiment140.py"]