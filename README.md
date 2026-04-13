

=======
# Orderbook Producer -> Kafka

Config in `.env`: `KAFKA_BROKER=kafka:9092` `KAFKA_TOPIC=orderbook.btcusdt` `PRODUCER_CLIENT_ID=orderbook-producer`

<span style="color:#16a34a"><strong>START</strong></span>

```bash
docker compose up --build
```

```bash
chmod +x init/create-topics.sh
```

<span style="color:#0ea5e9"><strong>CHECK</strong></span>

```bash
docker compose ps
docker compose logs -f producer
docker compose logs -f init-topics
```

### Topic initialization
This project creates Kafka topics automatically using an `init-topics` service.

Topics are listed in `config/topics.txt` using the format:
`<topic_name> <partitions> <replication_factor>`

On startup, `init-topics` waits for Kafka to become healthy, then creates all topics with `--if-not-exists`.

<span style="color:#f59e0b"><strong>VERIFY TOPICS</strong></span>

```bash
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --list"
```

Verify:
```bash
docker compose logs -f init-topics
docker exec -it kafka bash -lc "kafka-topics --bootstrap-server kafka:9092 --list"
```

<span style="color:#a855f7"><strong>CONSUME 5 MESSAGES</strong></span>

```bash
docker exec -it kafka bash -lc "kafka-console-consumer --bootstrap-server kafka:9092 --topic orderbook.btcusdt --max-messages 5"
 

```

<span style="color:#ef4444"><strong>STOP</strong></span>

```bash
docker compose down
```
>>>>>>> Initial commit
