#!/bin/bash
set -e

echo "Waiting for Kafka..."
until kafka-topics --bootstrap-server kafka:9092 --list >/dev/null 2>&1; do
  sleep 2
done

echo "Creating topics from /config/topics.txt ..."

# Read file, strip Windows CRLF, then process
sed 's/\r$//' /config/topics.txt | while read -r topic partitions replication; do
  [[ -z "$topic" || "$topic" =~ ^# ]] && continue

  kafka-topics \
    --bootstrap-server kafka:9092 \
    --create --if-not-exists \
    --topic "$topic" \
    --partitions "${partitions:-1}" \
    --replication-factor "${replication:-1}"
done

echo "Topics now:"
kafka-topics --bootstrap-server kafka:9092 --list
echo "Done."
