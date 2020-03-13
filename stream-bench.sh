#!/bin/bash
# Copyright 2015, Yahoo Inc.
# Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
set -o pipefail
set -o errtrace
set -o nounset
set -o errexit

LEIN=${LEIN:-lein}
MVN=${MVN:-mvn}
GIT=${GIT:-git}
MAKE=${MAKE:-make}

KAFKA_VERSION=${KAFKA_VERSION:-"0.8.2.1"}
REDIS_VERSION=${REDIS_VERSION:-"4.0.11"}
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.11"}
SCALA_SUB_VERSION=${SCALA_SUB_VERSION:-"12"}
STORM_VERSION=${STORM_VERSION:-"1.2.2"}
FLINK_VERSION=${FLINK_VERSION:-"1.6.0"}
SPARK_VERSION=${SPARK_VERSION:-"2.3.1"}

STORM_DIR="apache-storm-$STORM_VERSION"
REDIS_DIR="redis-$REDIS_VERSION"
KAFKA_DIR="kafka_$SCALA_BIN_VERSION-$KAFKA_VERSION"
FLINK_DIR="flink-$FLINK_VERSION"
SPARK_DIR="spark-$SPARK_VERSION-bin-hadoop2.7"

#Get one of the closet apache mirrors
APACHE_MIRROR=$"https://archive.apache.org/dist"

ZK_HOST="localhost"
ZK_PORT="2181"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"
GENERATOR_HOST="localhost"
GENERATOR_PORT=9000
TOPIC=${TOPIC:-"ad-events"}
PARTITIONS=${PARTITIONS:-1}
LOAD=${LOAD:-1000}
CONF_FILE=./conf/localConf.yaml
TEST_TIME=${TEST_TIME:-240}

pid_match() {
   local VAL=`ps -aef | grep "$1" | grep -v grep | awk '{print $2}'`
   echo $VAL
}

start_if_needed() {
  local match="$1"
  shift
  local name="$1"
  shift
  local sleep_time="$1"
  shift
  local PID=`pid_match "$match"`

  if [[ "$PID" -ne "" ]];
  then
    echo "$name is already running..."
  else
    "$@" &
    sleep $sleep_time
  fi
}

stop_if_needed() {
  local match="$1"
  local name="$2"
  local PID=`pid_match "$match"`
  if [[ "$PID" -ne "" ]];
  then
    kill "$PID"
    sleep 1
    local CHECK_AGAIN=`pid_match "$match"`
    if [[ "$CHECK_AGAIN" -ne "" ]];
    then
      kill -9 "$CHECK_AGAIN"
    fi
  else
    echo "No $name instance found to stop"
  fi
}

fetch_untar_file() {
  local FILE="download-cache/$1"
  local URL=$2
  if [[ -e "$FILE" ]];
  then
    echo "Using cached File $FILE"
  else
	mkdir -p download-cache/
    WGET=`whereis wget`
    CURL=`whereis curl`
    if [ -n "$WGET" ];
    then
      wget -O "$FILE" "$URL"
    elif [ -n "$CURL" ];
    then
      curl -o "$FILE" "$URL"
    else
      echo "Please install curl or wget to continue.";
      exit 1
    fi
  fi
  tar -xzvf "$FILE"
}

create_kafka_topic() {
    local count=`$KAFKA_DIR/bin/kafka-topics.sh --describe --zookeeper "$ZK_CONNECTIONS" --topic $TOPIC 2>/dev/null | grep -c $TOPIC`
    if [[ "$count" = "0" ]];
    then
        $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper "$ZK_CONNECTIONS" --replication-factor 1 --partitions $PARTITIONS --topic $TOPIC
    else
        echo "Kafka topic $TOPIC already exists"
    fi
}

run() {
  OPERATION=$1
  if [ "SETUP" = "$OPERATION" ];
  then
    $GIT clean -fd

    echo 'kafka.brokers:' > $CONF_FILE
    echo '    - "localhost"' >> $CONF_FILE
    echo >> $CONF_FILE
    echo 'zookeeper.servers:' >> $CONF_FILE
    echo '    - "'$ZK_HOST'"' >> $CONF_FILE
    echo >> $CONF_FILE
    echo 'kafka.port: 9092' >> $CONF_FILE
	echo 'zookeeper.port: '$ZK_PORT >> $CONF_FILE
	echo 'redis.host: "localhost"' >> $CONF_FILE
	echo 'kafka.topic: "'$TOPIC'"' >> $CONF_FILE
	echo 'kafka.partitions: '$PARTITIONS >> $CONF_FILE
	echo 'process.hosts: 1' >> $CONF_FILE
	echo 'process.cores: 4' >> $CONF_FILE
	echo 'storm.workers: 1' >> $CONF_FILE
	echo 'storm.ackers: 2' >> $CONF_FILE
	echo 'spark.batchtime: 2000' >> $CONF_FILE
	
    $MVN clean install -Dkafka.version="$KAFKA_VERSION" -Dflink.version="$FLINK_VERSION" -Dscala.binary.version="$SCALA_BIN_VERSION" -Dscala.version="$SCALA_BIN_VERSION.$SCALA_SUB_VERSION"

    #Fetch and build Redis
    REDIS_FILE="$REDIS_DIR.tar.gz"
    fetch_untar_file "$REDIS_FILE" "http://download.redis.io/releases/$REDIS_FILE"

    cd $REDIS_DIR
    $MAKE
    cd ..

    #Fetch Storm
    STORM_FILE="$STORM_DIR.tar.gz"
    fetch_untar_file "$STORM_FILE" "$APACHE_MIRROR/storm/$STORM_DIR/$STORM_FILE"
  elif [ "MVN_BUILD" = "$OPERATION" ];
  then
    $MVN clean install -Dspark.version="$SPARK_VERSION" -Dkafka.version="$KAFKA_VERSION" -Dflink.version="$FLINK_VERSION" -Dstorm.version="$STORM_VERSION" -Dscala.binary.version="$SCALA_BIN_VERSION" -Dscala.version="$SCALA_BIN_VERSION.$SCALA_SUB_VERSION"
  elif [ "START_REDIS" = "$OPERATION" ];
  then
    start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server"
  elif [ "STOP_REDIS" = "$OPERATION" ];
  then
    stop_if_needed redis-server Redis
    rm -f dump.rdb
  elif [ "START_FLINK" = "$OPERATION" ];
  then
    start_if_needed org.apache.flink.runtime.jobmanager.JobManager Flink 1 $FLINK_DIR/bin/start-cluster.sh
  elif [ "STOP_FLINK" = "$OPERATION" ];
  then
    $FLINK_DIR/bin/stop-cluster.sh
  elif [ "START_LOAD" = "$OPERATION" ];
  then
    cd flink-injector
    start_if_needed SocketInjector "Load Generation" 1 $MVN exec:java -Dexec.mainClass=SocketInjector  -Dexec.args="--redis localhost --port $GENERATOR_PORT --throughput $LOAD"
    cd ..
  elif [ "STOP_LOAD" = "$OPERATION" ];
  then
    stop_if_needed SocketInjector "Load Generation"
    cd data
    $LEIN run -g --configPath ../$CONF_FILE || true
    cd ..
  elif [ "START_FLINK_PROCESSING" = "$OPERATION" ];
  then
    "$FLINK_DIR/bin/flink" run ./flink-benchmarks/target/flink-benchmarks-0.1.0.jar --hostname $GENERATOR_HOST --port $GENERATOR_PORT --confPath $CONF_FILE &
    sleep 3
  elif [ "STOP_FLINK_PROCESSING" = "$OPERATION" ];
  then
    FLINK_ID=`"$FLINK_DIR/bin/flink" list | grep 'Flink Streaming Job' | awk '{print $4}'; true`
    if [ "$FLINK_ID" == "" ];
	then
	  echo "Could not find streaming job to kill"
    else
      "$FLINK_DIR/bin/flink" cancel $FLINK_ID
      sleep 3
    fi
  elif [ "FLINK_TEST" = "$OPERATION" ];
  then
#    run "START_ZK"
    run "START_REDIS"
    run "START_FLINK"
    run "START_LOAD"
    run "START_FLINK_PROCESSING"
    sleep $TEST_TIME
    run "STOP_FLINK_PROCESSING"
    run "STOP_LOAD"
    run "STOP_FLINK"
    run "STOP_REDIS"
#    run "STOP_ZK"
  elif [ "STOP_ALL" = "$OPERATION" ];
  then
    run "STOP_LOAD"
    run "STOP_FLINK_PROCESSING"
    run "STOP_FLINK"
    run "STOP_REDIS"
#    run "STOP_ZK"
  else
    if [ "HELP" != "$OPERATION" ];
    then
      echo "UNKOWN OPERATION '$OPERATION'"
      echo
    fi
    echo "Supported Operations:"
    echo "MVN_BUILD: build the benchmarks"
    echo "SETUP: download and setup dependencies for running a single node test"
    echo "START_ZK: run a single node ZooKeeper instance on local host in the background"
    echo "STOP_ZK: kill the ZooKeeper instance"
    echo "START_REDIS: run a redis instance in the background"
    echo "STOP_REDIS: kill the redis instance"
    echo "START_LOAD: run kafka load generation"
    echo "STOP_LOAD: kill kafka load generation"
    echo "START_FLINK: run flink processes"
    echo "STOP_FLINK: kill flink processes"
    echo
    echo "START_FLINK_PROCESSING: run the flink test processing"
    echo "STOP_FLINK_PROCESSSING: kill the flink test processing"
    echo
    echo "FLINK_TEST: run flink test (assumes SETUP is done)"
    echo "STOP_ALL: stop everything"
    echo
    echo "HELP: print out this message"
    echo
    exit 1
  fi
}

if [ $# -lt 1 ];
then
  run "HELP"
else
  while [ $# -gt 0 ];
  do
    run "$1"
    shift
  done
fi
