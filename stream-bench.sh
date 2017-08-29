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

KAFKA_VERSION=${KAFKA_VERSION:-"0.10.2.0"}
REDIS_VERSION=${REDIS_VERSION:-"3.2.8"}
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.11"}
SCALA_SUB_VERSION=${SCALA_SUB_VERSION:-"11"}
STORM_VERSION=${STORM_VERSION:-"0.9.7"}
FLINK_VERSION=${FLINK_VERSION:-"1.2.0"}
SPARK_VERSION=${SPARK_VERSION:-"2.1.0"}
APEX_VERSION=${APEX_VERSION:-"3.5.0"}

STORM_DIR="apache-storm-$STORM_VERSION"
REDIS_DIR="redis-$REDIS_VERSION"
KAFKA_DIR="kafka_$SCALA_BIN_VERSION-$KAFKA_VERSION"
FLINK_DIR="flink-$FLINK_VERSION"
SPARK_DIR="spark-$SPARK_VERSION-bin-hadoop2.7"
APEX_DIR="apache-apex-core-$APEX_VERSION"

#Get one of the closet apache mirrors
APACHE_MIRROR=$(curl 'https://www.apache.org/dyn/closer.cgi' |   grep -o '<strong>[^<]*</strong>' |   sed 's/<[^>]*>//g' |   head -1)

ZK_HOST="localhost"
ZK_PORT="2181"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"
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
	
    $MVN clean install -Dspark.version="$SPARK_VERSION" -Dkafka.version="$KAFKA_VERSION" -Dflink.version="$FLINK_VERSION" -Dstorm.version="$STORM_VERSION" -Dscala.binary.version="$SCALA_BIN_VERSION" -Dscala.version="$SCALA_BIN_VERSION.$SCALA_SUB_VERSION" -Dapex.version="$APEX_VERSION"

    #Fetch and build Redis
    REDIS_FILE="$REDIS_DIR.tar.gz"
    fetch_untar_file "$REDIS_FILE" "http://download.redis.io/releases/$REDIS_FILE"

    cd $REDIS_DIR
    $MAKE
    cd ..

    #Fetch Apex
    APEX_FILE="$APEX_DIR.tgz.gz"
    fetch_untar_file "$APEX_FILE" "$APACHE_MIRROR/apex/apache-apex-core-$APEX_VERSION/apache-apex-core-$APEX_VERSION-source-release.tar.gz"
    cd $APEX_DIR
    $MVN clean install -DskipTests
    cd ..

    #Fetch Kafka
    KAFKA_FILE="$KAFKA_DIR.tgz"
    fetch_untar_file "$KAFKA_FILE" "$APACHE_MIRROR/kafka/$KAFKA_VERSION/$KAFKA_FILE"

    #Fetch Storm
    STORM_FILE="$STORM_DIR.tar.gz"
    fetch_untar_file "$STORM_FILE" "$APACHE_MIRROR/storm/$STORM_DIR/$STORM_FILE"

    #Fetch Flink
    FLINK_FILE="$FLINK_DIR-bin-hadoop27-scala_${SCALA_BIN_VERSION}.tgz"
    fetch_untar_file "$FLINK_FILE" "$APACHE_MIRROR/flink/flink-$FLINK_VERSION/$FLINK_FILE"

    #Fetch Spark
    SPARK_FILE="$SPARK_DIR.tgz"
    fetch_untar_file "$SPARK_FILE" "$APACHE_MIRROR/spark/spark-$SPARK_VERSION/$SPARK_FILE"

  elif [ "START_ZK" = "$OPERATION" ];
  then
    start_if_needed dev_zookeeper ZooKeeper 10 "$STORM_DIR/bin/storm" dev-zookeeper
  elif [ "STOP_ZK" = "$OPERATION" ];
  then
    stop_if_needed dev_zookeeper ZooKeeper
    rm -rf /tmp/dev-storm-zookeeper
  elif [ "START_REDIS" = "$OPERATION" ];
  then
    start_if_needed redis-server Redis 1 "$REDIS_DIR/src/redis-server"
    cd data
    $LEIN run -n --configPath ../conf/benchmarkConf.yaml
    cd ..
  elif [ "STOP_REDIS" = "$OPERATION" ];
  then
    stop_if_needed redis-server Redis
    rm -f dump.rdb
  elif [ "START_STORM" = "$OPERATION" ];
  then
    start_if_needed daemon.name=nimbus "Storm Nimbus" 3 "$STORM_DIR/bin/storm" nimbus
    start_if_needed daemon.name=supervisor "Storm Supervisor" 3 "$STORM_DIR/bin/storm" supervisor
    start_if_needed daemon.name=ui "Storm UI" 3 "$STORM_DIR/bin/storm" ui
    start_if_needed daemon.name=logviewer "Storm LogViewer" 3 "$STORM_DIR/bin/storm" logviewer
    sleep 20
  elif [ "STOP_STORM" = "$OPERATION" ];
  then
    stop_if_needed daemon.name=nimbus "Storm Nimbus"
    stop_if_needed daemon.name=supervisor "Storm Supervisor"
    stop_if_needed daemon.name=ui "Storm UI"
    stop_if_needed daemon.name=logviewer "Storm LogViewer"
  elif [ "START_KAFKA" = "$OPERATION" ];
  then
    start_if_needed kafka\.Kafka Kafka 10 "$KAFKA_DIR/bin/kafka-server-start.sh" "$KAFKA_DIR/config/server.properties"
    create_kafka_topic
  elif [ "STOP_KAFKA" = "$OPERATION" ];
  then
    stop_if_needed kafka\.Kafka Kafka
    rm -rf /tmp/kafka-logs/
  elif [ "START_FLINK" = "$OPERATION" ];
  then
    start_if_needed org.apache.flink.runtime.jobmanager.JobManager Flink 1 $FLINK_DIR/bin/start-local.sh
  elif [ "STOP_FLINK" = "$OPERATION" ];
  then
    $FLINK_DIR/bin/stop-local.sh
  elif [ "START_SPARK" = "$OPERATION" ];
  then
    start_if_needed org.apache.spark.deploy.master.Master SparkMaster 5 $SPARK_DIR/sbin/start-master.sh -h localhost -p 7077
    start_if_needed org.apache.spark.deploy.worker.Worker SparkSlave 5 $SPARK_DIR/sbin/start-slave.sh spark://localhost:7077
  elif [ "STOP_SPARK" = "$OPERATION" ];
  then
    stop_if_needed org.apache.spark.deploy.master.Master SparkMaster
    stop_if_needed org.apache.spark.deploy.worker.Worker SparkSlave
    sleep 3
  elif [ "START_LOAD" = "$OPERATION" ];
  then
    cd data
    start_if_needed leiningen.core.main "Load Generation" 1 $LEIN run -r -t $LOAD --configPath ../$CONF_FILE
    cd ..
  elif [ "STOP_LOAD" = "$OPERATION" ];
  then
    stop_if_needed leiningen.core.main "Load Generation"
    cd data
    $LEIN run -g --configPath ../$CONF_FILE || true
    cd ..
  elif [ "START_STORM_TOPOLOGY" = "$OPERATION" ];
  then
    "$STORM_DIR/bin/storm" jar ./storm-benchmarks/target/storm-benchmarks-0.1.0.jar storm.benchmark.AdvertisingTopology test-topo -conf $CONF_FILE
    sleep 15
  elif [ "STOP_STORM_TOPOLOGY" = "$OPERATION" ];
  then
    "$STORM_DIR/bin/storm" kill -w 0 test-topo || true
    sleep 10
  elif [ "START_SPARK_PROCESSING" = "$OPERATION" ];
  then
    "$SPARK_DIR/bin/spark-submit" --master spark://localhost:7077 --class spark.benchmark.KafkaRedisAdvertisingStream ./spark-benchmarks/target/spark-benchmarks-0.1.0.jar "$CONF_FILE" &
    sleep 5
  elif [ "STOP_SPARK_PROCESSING" = "$OPERATION" ];
  then
    stop_if_needed spark.benchmark.KafkaRedisAdvertisingStream "Spark Client Process"
  elif [ "START_FLINK_PROCESSING" = "$OPERATION" ];
  then
    "$FLINK_DIR/bin/flink" run ./flink-benchmarks/target/flink-benchmarks-0.1.0.jar --confPath $CONF_FILE &
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
  elif [ "START_APEX" = "$OPERATION" ];
      then
      "$APEX_DIR/engine/src/main/scripts/apex" -e "launch -local -conf ./conf/apex.xml ./apex-benchmarks/target/apex_benchmark-1.0-SNAPSHOT.apa -exactMatch Apex_Benchmark"
             sleep 5
  elif [ "STOP_APEX" = "$OPERATION" ];
       then
       pkill -f apex_benchmark
  elif [ "START_APEX_ON_YARN" = "$OPERATION" ];
       then
        "$APEX_DIR/engine/src/main/scripts/apex" -e "launch ./apex-benchmarks/target/apex_benchmark-1.0-SNAPSHOT.apa -conf ./conf/apex.xml -exactMatch Apex_Benchmark"
  elif [ "STOP_APEX_ON_YARN" = "$OPERATION" ];
       then
       APP_ID=`"$APEX_DIR/engine/src/main/scripts/apex" -e "list-apps" | grep id | awk '{ print $2 }'| cut -c -1 ; true`
       if [ "APP_ID" == "" ];
       then
         echo "Could not find streaming job to kill"
       else
        "$APEX_DIR/engine/src/main/scripts/apex" -e "kill-app $APP_ID"
         sleep 3
       fi
  elif [ "STORM_TEST" = "$OPERATION" ];
  then
    run "START_ZK"
    run "START_REDIS"
    run "START_KAFKA"
    run "START_STORM"
    run "START_STORM_TOPOLOGY"
    run "START_LOAD"
    sleep $TEST_TIME
    run "STOP_LOAD"
    run "STOP_STORM_TOPOLOGY"
    run "STOP_STORM"
    run "STOP_KAFKA"
    run "STOP_REDIS"
    run "STOP_ZK"
  elif [ "FLINK_TEST" = "$OPERATION" ];
  then
    run "START_ZK"
    run "START_REDIS"
    run "START_KAFKA"
    run "START_FLINK"
    run "START_FLINK_PROCESSING"
    run "START_LOAD"
    sleep $TEST_TIME
    run "STOP_LOAD"
    run "STOP_FLINK_PROCESSING"
    run "STOP_FLINK"
    run "STOP_KAFKA"
    run "STOP_REDIS"
    run "STOP_ZK"
  elif [ "SPARK_TEST" = "$OPERATION" ];
  then
    run "START_ZK"
    run "START_REDIS"
    run "START_KAFKA"
    run "START_SPARK"
    run "START_SPARK_PROCESSING"
    run "START_LOAD"
    sleep $TEST_TIME
    run "STOP_LOAD"
    run "STOP_SPARK_PROCESSING"
    run "STOP_SPARK"
    run "STOP_KAFKA"
    run "STOP_REDIS"
    run "STOP_ZK"
 elif [ "APEX_TEST" = "$OPERATION" ];
  then
    run "START_ZK"
    run "START_REDIS"
    run "START_KAFKA"
    run "START_APEX"
    run "START_LOAD"
    sleep $TEST_TIME
    run "STOP_LOAD"
    run "STOP_APEX"
    run "STOP_KAFKA"
    run "STOP_REDIS"
    run "STOP_ZK"
  elif [ "STOP_ALL" = "$OPERATION" ];
  then
    run "STOP_LOAD"
    run "STOP_SPARK_PROCESSING"
    run "STOP_SPARK"
    run "STOP_FLINK_PROCESSING"
    run "STOP_FLINK"
    run "STOP_STORM_TOPOLOGY"
    run "STOP_STORM"
    run "STOP_KAFKA"
    run "STOP_REDIS"
    run "STOP_ZK"
  else
    if [ "HELP" != "$OPERATION" ];
    then
      echo "UNKOWN OPERATION '$OPERATION'"
      echo
    fi
    echo "Supported Operations:"
    echo "SETUP: download and setup dependencies for running a single node test"
    echo "START_ZK: run a single node ZooKeeper instance on local host in the background"
    echo "STOP_ZK: kill the ZooKeeper instance"
    echo "START_REDIS: run a redis instance in the background"
    echo "STOP_REDIS: kill the redis instance"
    echo "START_KAFKA: run kafka in the background"
    echo "STOP_KAFKA: kill kafka"
    echo "START_LOAD: run kafka load generation"
    echo "STOP_LOAD: kill kafka load generation"
    echo "START_STORM: run storm daemons in the background"
    echo "STOP_STORM: kill the storm daemons"
    echo "START_FLINK: run flink processes"
    echo "STOP_FLINK: kill flink processes"
    echo "START_SPARK: run spark processes"
    echo "STOP_SPARK: kill spark processes"
    echo "START_APEX: run the Apex test processing"
    echo "STOP_APEX: kill the Apex test processing"
    echo 
    echo "START_STORM_TOPOLOGY: run the storm test topology"
    echo "STOP_STORM_TOPOLOGY: kill the storm test topology"
    echo "START_FLINK_PROCESSING: run the flink test processing"
    echo "STOP_FLINK_PROCESSSING: kill the flink test processing"
    echo "START_SPARK_PROCESSING: run the spark test processing"
    echo "STOP_SPARK_PROCESSSING: kill the spark test processing"
    echo
    echo "STORM_TEST: run storm test (assumes SETUP is done)"
    echo "FLINK_TEST: run flink test (assumes SETUP is done)"
    echo "SPARK_TEST: run spark test (assumes SETUP is done)"
    echo "APEX_TEST: run Apex test (assumes SETUP is done)"
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
