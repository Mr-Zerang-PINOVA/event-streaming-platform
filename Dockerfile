FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -U pip setuptools wheel \
 && pip install --no-cache-dir -r /app/requirements.txt

COPY collectors/ /app/collectors/
COPY contracts/ /app/contracts/
COPY processors/ /app/processors/
COPY producers/ /app/producers/
COPY config/ /app/config/
COPY main.py /app/main.py

CMD ["python", "-u", "/app/main.py", "--config", "/app/config/config.docker.yaml"]
