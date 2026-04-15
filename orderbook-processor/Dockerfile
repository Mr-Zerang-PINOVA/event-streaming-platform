FROM python:3.11-slim

WORKDIR /app

COPY orderbook-processor/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY contracts/ /app/contracts/
COPY config/ /app/config/
COPY orderbook-processor/*.py /app/

CMD ["python", "-u", "/app/processor.py"]
