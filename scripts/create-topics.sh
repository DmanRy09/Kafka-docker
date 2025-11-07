#!/bin/bash

# Create Kafka topics for payments and sales
docker exec kafka-docker-app_kafka_1 kafka-topics.sh --create --topic payments --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
docker exec kafka-docker-app_kafka_1 kafka-topics.sh --create --topic sales --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

# List the created topics
docker exec kafka-docker-app_kafka_1 kafka-topics.sh --list --bootstrap-server localhost:9092