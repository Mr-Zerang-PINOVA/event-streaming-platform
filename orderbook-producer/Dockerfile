FROM python:3.11-slim

WORKDIR /app

# OS deps needed for confluent-kafka (build-from-source fallback) + runtime
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    python3-dev \
    librdkafka-dev \
    pkg-config \
  && rm -rf /var/lib/apt/lists/*

COPY orderbook-producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -U pip setuptools wheel \
 && pip install --no-cache-dir -r /app/requirements.txt

COPY orderbook-producer/ /app/orderbook-producer/
COPY config/ /app/config/

CMD ["python", "-u", "/app/orderbook-producer/producer.py"]
