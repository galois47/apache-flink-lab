#!/bin/bash
set -e

DATA_DIR="/var/lib/kafka/data"
mkdir -p "$DATA_DIR"

# Generate cluster ID on first run
if [ ! -f "$DATA_DIR/meta.properties" ]; then
  export CLUSTER_ID=$(kafka-storage random-uuid)
  echo "Generated CLUSTER_ID: $CLUSTER_ID"
fi

# Start Kafka in the background
/etc/confluent/docker/run &
KAFKA_PID=$!

echo "Waiting for Kafka to be ready..."
max_attempts=60
attempt=0

while [ $attempt -lt $max_attempts ]; do
  if kafka-broker-api-versions --bootstrap-server localhost:9092 >/dev/null 2>&1; then
    echo "Kafka is ready! Initializing topics..."
    break
  fi
  attempt=$((attempt + 1))
  echo "Attempt $attempt/$max_attempts - waiting for Kafka..."
  sleep 1
done

if [ $attempt -eq $max_attempts ]; then
  echo "ERROR: Kafka did not become ready in time"
  kill $KAFKA_PID
  exit 1
fi

# Create topics (idempotent)
for topic in input-topic output-topic; do
  if kafka-topics --bootstrap-server localhost:9092 --list | grep -q "^${topic}$"; then
    echo "${topic} already exists"
  else
    echo "Creating ${topic}..."
    kafka-topics --create --topic "$topic" --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
  fi
done

echo "Topics initialized successfully!"

# Keep Kafka running
wait $KAFKA_PID
