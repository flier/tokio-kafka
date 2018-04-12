#!/bin/bash -e

TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKER_DIR=${TEST_DIR}/docker

case "$OSTYPE" in
  linux*)
    IP_ADDRESS=`hostname -I | awk '{print $1}'`
    ;;
  darwin*)
    IP_ADDRESSES=(`ifconfig | grep "inet " | grep -Fv 127.0.0.1 | awk '{print $2}'`)
    IP_ADDRESS=${IP_ADDRESSES[0]}
    ;;
  *)
    echo "unknown: $OSTYPE"
    ;;
esac

pushd ${DOCKER_DIR}/v0.8

envfile=.env

rm $envfile

echo "###  DOCKER-COMPOSE ENVIRONMENT VARIABLES AS OF $(date +"%Y-%m-%d @ %H-%M-%S")" >> $envfile
echo "IP_ADDRESS=${IP_ADDRESS}" >> $envfile

cat $envfile

docker-compose kill
docker-compose up -d

RUST_LOG=tokio KAFKA_BROKERS=${IP_ADDRESS}:9092 cargo test --features "integration_test"

popd
