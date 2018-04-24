#!/bin/bash -e

KAFKA_HOME=/opt/kafka
KAFKA_TOPICS="$KAFKA_HOME/bin/kafka-topics.sh --zookeeper zookeeper:2181"
KAFKA_CONSOLE_PRODUCER="$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list localhost:9092"

until $KAFKA_TOPICS --list  | grep bar; do
    echo "Kafka is unavailable - sleeping"
    sleep 1
done

echo Setup integration testing @ $KAFKA_HOME

for i in `seq 0 9`; do
    echo $i | $KAFKA_CONSOLE_PRODUCER --topic foo;
    echo $i | $KAFKA_CONSOLE_PRODUCER --topic bar;
done

$KAFKA_TOPICS --describe
