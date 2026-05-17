#!/bin/bash
# Creates all required Kafka topics
# Run this once after Kafka is healthy

KAFKA_CONTAINER="autoops-kafka"

echo "Creating Kafka topics..."

docker exec $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic incidents \
  --partitions 3 \
  --replication-factor 1

docker exec $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic remediation-results \
  --partitions 3 \
  --replication-factor 1

docker exec $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server localhost:9092 \
  --create --if-not-exists \
  --topic escalations \
  --partitions 1 \
  --replication-factor 1

echo "Topics created. Listing all topics:"
docker exec $KAFKA_CONTAINER kafka-topics \
  --bootstrap-server localhost:9092 \
  --list