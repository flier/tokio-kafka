#!/bin/bash -e

TEST_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
DOCKER_DIR=$TEST_DIR/docker

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

function test {
    echo Testing Kafka $1 @ $version

    pushd $DOCKER_DIR/$1

    envfile=.env

    echo "###  DOCKER-COMPOSE ENVIRONMENT VARIABLES AS OF $(date +"%Y-%m-%d @ %H-%M-%S")" > $envfile
    echo "IP_ADDRESS=$IP_ADDRESS" >> $envfile

    cat $envfile

    docker-compose kill
    docker-compose rm -f
    docker-compose build
    docker-compose up -d

    docker-compose exec kafka /tmp/setup-topics.sh

    for i in {1..5}
    do
        sleep 1
        echo '.'
    done

    RUST_LOG=tokio KAFKA_BROKERS=$IP_ADDRESS:9092 cargo test --features "integration_test"

    docker-compose down

    popd
}

POSITIONAL=()
while [[ $# -gt 0 ]]
do
  arg="$1"
  case $arg in
    v0.8|v0.9|v0.10|0.11|v1.0|v1.1)
      test $arg
      shift # past argument
    ;;
    all)
      for version in $DOCKER_DIR/v*/; do
        test $(basename $version)
      done
      shift # past argument
    ;;
    *)    # unknown option
      POSITIONAL+=("$1") # save it in an array for later
      shift # past argument
    ;;
  esac
done
