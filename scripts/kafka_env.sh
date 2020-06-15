#!bin/bash
# Set up env for cluster to run various benchmarks
export PARTITIONS=5
KAFKA_VERSION=${KAFKA_VERSION:-"2.4.1"}
SCALA_VERSION=${SCALA_VERSION:-"2.12"}

sed -i -e 's/broker.id=0/broker.id=$i/' ~/streaming-benchmarks/kafka_$SCALA_VERSION-$KAFKA_VERSION/config/server.properties

sed -i -e 's/zookeeper.connect=localhost:2181/zookeeper.connect=10.227.218.254:2181,10.227.218.253:2181,10.227.218.252:2181/' ~/streaming-benchmarks/kafka_$SCALA_VERSION-$KAFKA_VERSION/config/server.properties
