#!/bin/bash
set -e

cd "$(dirname "$0")"

echo "=== HVAC E2E Integration Test ==="
echo "    MQTT -> Varpulis -> Kafka"
echo ""

cleanup() {
    echo ""
    echo "Cleaning up..."
    docker compose down -v --remove-orphans 2>/dev/null || true
}
trap cleanup EXIT

# Start infrastructure
echo "Starting infrastructure (Mosquitto, Zookeeper, Kafka)..."
docker compose up -d mosquitto zookeeper kafka

echo "Waiting for Kafka to be healthy..."
docker compose up kafka-setup

# Build and start Varpulis
echo ""
echo "Building and starting Varpulis engine..."
docker compose up -d --build varpulis
echo "Waiting for Varpulis to connect to MQTT..."
sleep 5

# Show Varpulis logs for debugging
echo ""
echo "--- Varpulis logs ---"
docker compose logs varpulis
echo "--- End Varpulis logs ---"
echo ""

# Run producer
echo "Running producer (publishing 10 events)..."
docker compose run --rm producer

# Wait for processing pipeline
echo ""
echo "Waiting for events to flow through pipeline..."
sleep 5

# Run consumer/validator
echo ""
echo "Running consumer/validator..."
docker compose run --rm consumer
RESULT=$?

if [ $RESULT -eq 0 ]; then
    echo ""
    echo "=== TEST PASSED ==="
else
    echo ""
    echo "=== TEST FAILED ==="
    echo ""
    echo "--- Varpulis logs ---"
    docker compose logs varpulis
fi

exit $RESULT
