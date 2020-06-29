#!/bin/bash
ROOT="~/streaming-benchmarks"
BEAM_VERSION=${BEAM_VERSION:-"2.20.0"}
KAFKA_VERSION=${KAFKA_VERSION:-"2.4.1"}
REDIS_VERSION=${REDIS_VERSION:-"6.0.1"}
SCALA_BIN_VERSION=${SCALA_BIN_VERSION:-"2.12"}
SCALA_SUB_VERSION=${SCALA_SUB_VERSION:-"11"}
STORM_VERSION=${STORM_VERSION:-"2.1.0"}
FLINK_VERSION=${FLINK_VERSION:-"1.10.0"}
SPARK_VERSION=${SPARK_VERSION:-"3.0.0"}
HADOOP_FLINK_BUNDLE_VERSION=${HADOOP_VERSION:-"2.8.3-0.10"}
YJAVA_HOME=${YJAVA_HOME:-"/home/y/share/yjava_jdk/java"}

STORM_DIR="$ROOT/apache-storm-$STORM_VERSION"
REDIS_DIR="$ROOT/redis-$REDIS_VERSION"
KAFKA_DIR="$ROOT/kafka_$SCALA_BIN_VERSION-$KAFKA_VERSION"
FLINK_DIR="$ROOT/flink-$FLINK_VERSION"
SPARK_DIR="$ROOT/spark-$SPARK_VERSION-bin-hadoop2.7"

ZK_HOST="localhost"
ZK_PORT="2181"
ZK_CONNECTIONS="$ZK_HOST:$ZK_PORT"
TOPIC=${TOPIC:-"ad-events"}
PARTITIONS=${PARTITIONS:-5}
LOAD=${LOAD:-50000}
CONF_FILE="$ROOT/conf/localConf.yaml"
TEST_TIME=${TEST_TIME:-1800}
ADMIN_HOST=${ADMIN_HOST:-"stbl1230n00.blue.ygrid.yahoo.com"}
ZK_DIR=${ZK_DIR:-"/tmp/zookeeper/"}
PRODUCERS=${PRODUCERS:-10}

# TODO: May be there is a way to avoid touching yubikey for initial setup for few ops and yinst installs
# TODO: Replace for loops based on file list or env variables
setup_lein() {
  for i in {0..9};
  do
    scp ./lein stbl1230n0$i.blue.ygrid.yahoo.com:~
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com 'mkdir -p ~/.lein/self-installs/'
    scp ./leiningen-2.9.3-standalone.jar stbl1230n0$i.blue.ygrid.yahoo.com:~/.lein/self-installs/
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com 'sudo cp ~/lein /usr/local/bin'
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com 'sudo chmod a+x /usr/local/bin/lein'
  done

  #mvn zip and upload
  # TODO: Replace for loops based on file list or env variables
  cd ~
  zip -r m2.zip .m2
  for i in {0..9};
  do
    scp m2.zip stbl1230n0$i.blue.ygrid.yahoo.com:~
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com 'nohup unzip -o m2.zip > /dev/null 2>&1 &'
  done
  cd -
}

setup_zookeeper_quorum() {
  # TODO: Replace for loops based on file list or env variables
  for i in {5..7};
  do
    # Required for zookeeper server id's
    ZK_ID=`expr $i - 4`
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "mkdir -p $ZK_DIR"
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "echo $ZK_ID > $ZK_DIR/myid"
  done
}

setup_kafka_instances() {
  # TODO: Replace for loops based on file list or env variables
  for i in {5..9};
  do
    scp ./server.properties stbl1230n0$i.blue.ygrid.yahoo.com:$KAFKA_DIR/config/
    BROKER_ID=`expr $i - 5`
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "sed -i -e s/broker.id=0/broker.id=$BROKER_ID/ $KAFKA_DIR/config/server.properties > /dev/null 2>&1 &"
  done
}

start_zookeeper_quorum() {
  # TODO: Replace for loops based on file list or env variables
  for i in {5..7};
  do
    echo "Starting zookeeper on host stbl1230n0$i.blue.ygrid.yahoo.com"
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "JAVA_HOME=$YJAVA_HOME nohup $KAFKA_DIR/bin/zookeeper-server-start.sh $KAFKA_DIR/config/zookeeper.properties > /dev/null 2>&1 &"
  done
}

stop_zookeeper_quorum() {
  for i in {5..7};
  do
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "nohup sh $ROOT/stream-bench.sh STOP_ZK > /dev/null 2>&1 &"
  done
}

create_kafka_topic() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n05.blue.ygrid.yahoo.com "JAVA_HOME=$YJAVA_HOME nohup $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper $ZK_CONNECTIONS --replication-factor 1 --partitions $PARTITIONS --topic $TOPIC > /dev/null 2>&1 &"
}

start_kafka_instances() {
  # TODO: Replace for loops based on file list or env variables
  for i in {5..9};
  do
    echo "Starting kafka instance on host stbl1230n0$i.blue.ygrid.yahoo.com"
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "JAVA_HOME=$YJAVA_HOME nohup $KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/server.properties > /dev/null 2>&1 &"
  done
  create_kafka_topic
}

stop_kafka_instances() {
  for i in {5..9};
  do
    ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "KAFKA_DIR=$KAFKA_DIR nohup sh $ROOT/stream-bench.sh STOP_KAFKA > /dev/null 2>&1 &"
  done
}

# Storm start and stop
start_storm_cluster() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME nohup $STORM_DIR/bin/storm nimbus > /dev/null 2>&1 &"
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME nohup $STORM_DIR/bin/storm ui > /dev/null 2>&1 &"
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME nohup $STORM_DIR/bin/storm logviewer > /dev/null 2>&1 &"
  ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n01.blue.ygrid.yahoo.com "JAVA_HOME=$YJAVA_HOME nohup $STORM_DIR/bin/storm supervisor > /dev/null 2>&1 &"
  ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n02.blue.ygrid.yahoo.com "JAVA_HOME=$YJAVA_HOME nohup $STORM_DIR/bin/storm supervisor > /dev/null 2>&1 &"
}

start_storm_topology() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME nohup $STORM_DIR/bin/storm jar $ROOT/storm-benchmarks/target/storm-benchmarks-0.1.0.jar storm.benchmark.AdvertisingTopology test-topo -conf $CONF_FILE > /dev/null 2>&1"
  sleep 15
}

stop_storm_topology() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME nohup $STORM_DIR/bin/storm kill -w 0 test-topo  > /dev/null 2>&1 &"
  sleep 10
}

stop_storm_cluster() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "STORM_DIR=$STORM_DIR nohup sh $ROOT/stream-bench.sh STOP_STORM > /dev/null 2>&1 &"
  ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n01.blue.ygrid.yahoo.com "STORM_DIR=$STORM_DIR nohup sh $ROOT/stream-bench.sh STOP_STORM > /dev/null 2>&1 &"
  ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n02.blue.ygrid.yahoo.com "STORM_DIR=$STORM_DIR nohup sh $ROOT/stream-bench.sh STOP_STORM > /dev/null 2>&1 &"
}

# Flink start and stop
# Add slots in slave config file for flink
start_flink_cluster() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME nohup $FLINK_DIR/bin/start-cluster.sh > /dev/null 2>&1 &"
}

stop_flink_cluster() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME nohup $FLINK_DIR/bin/stop-cluster.sh > /dev/null 2>&1 &"
}

start_flink_topology() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME nohup $FLINK_DIR/bin/flink run $ROOT/flink-benchmarks/target/flink-benchmarks-0.1.0.jar --confPath $CONF_FILE > /dev/null 2>&1 &"
  echo "Starting flink topology"
}

stop_flink_topology() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME FLINK_DIR=$FLINK_DIR sh $ROOT/stream-bench.sh STOP_FLINK_PROCESSING > /dev/null 2>&1 &"
  echo "Stopping flink topology"
}

# Spark start and stop
# Check if we have slave config file
start_spark_cluster() {
  if [ "BEAM" = $1 ];
  then
    SPARK_VERSION=2.4.6
    SPARK_DIR="$ROOT/spark-$SPARK_VERSION-bin-hadoop2.7"
  fi
  printf "export JAVA_HOME=$JAVA_HOME\nexport SPARK_HOME=$SPARK_DIR\nexport SPARK_CONF_DIR=$SPARK_DIR/conf" > ./bashrc
  scp ./bashrc stbl1230n0$i.blue.ygrid.yahoo.com:~
  ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "mv bashrc ~/.bashrc"
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME SPARK_HOME=$SPARK_DIR SPARK_CONF_DIR=$SPARK_HOME/conf sh $SPARK_DIR/sbin/start-master.sh -h localhost -p 7077 > /dev/null 2>&1 &"
  sleep 30
  ssh -o StrictHostKeyChecking=no `whoami`@stbl1230n00.blue.ygrid.yahoo.com "JAVA_HOME=$YJAVA_HOME SPARK_HOME=$SPARK_DIR SPARK_CONF_DIR=$SPARK_HOME/conf sh $SPARK_DIR/sbin/start-slave.sh spark://localhost:7077 > /dev/null 2>&1 &"
  sleep 30
}

stop_spark_cluster() {
  ssh -o StrictHostKeyChecking=no `whoami`@stbl1230n00.blue.ygrid.yahoo.com "JAVA_HOME=$YJAVA_HOME sh $ROOT/stream-bench.sh STOP_SPARK  > /dev/null 2>&1 &"
}

start_spark_topology() {
  ssh -o StrictHostKeyChecking=no `whoami`@$ADMIN_HOST "$SPARK_DIR/bin/spark-submit --master spark://localhost:7077 --conf spark.executor.cores=6 --class spark.benchmark.dstream.KafkaRedisDStreamAdvertisingStream $ROOT/spark-dstream-benchmarks/target/spark-dstream-benchmarks-0.1.0.jar $CONF_FILE > /dev/null 2>&1 &"
}

stop_spark_topology() {
  ssh -o StrictHostKeyChecking=no `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME sh $ROOT/stream-bench.sh STOP_DSTREAM_SPARK_PROCESSING > /dev/null 2>&1 &"
}

start_ss_spark_topology() {
  ssh -o StrictHostKeyChecking=no `whoami`@$ADMIN_HOST "$SPARK_DIR/bin/spark-submit --master spark://localhost:7077 --class spark.benchmark.dstream.KafkaRedisSSContinuousAdvertisingStream $ROOT/spark-dstream-benchmarks/target/spark-dstream-benchmarks-0.1.0.jar $CONF_FILE > /dev/null 2>&1 &"
}

stop_ss_spark_topology() {
  ssh -o StrictHostKeyChecking=no `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME sh stream-bench.sh STOP_SS_SPARK_PROCESSING  > /dev/null 2>&1 &"
}

start_beam_spark_topology() {
  SPARK_VERSION=2.4.6
  SPARK_DIR="$ROOT/spark-$SPARK_VERSION-bin-hadoop2.7"
  ssh -o StrictHostKeyChecking=no `whoami`@$ADMIN_HOST "$SPARK_DIR/bin/spark-submit --master spark://localhost:7077 --conf spark.executor.cores=6 --class apache.beam.AdvertisingBeamStream $ROOT/apache-beam-validator/target/apache-beam-validator-0.1.0.jar --runner=SparkRunner --batchIntervalMillis=2000 --beamConf=/home/schintap/streaming-benchmarks/conf/localConf.yaml  > /dev/null 2>&1 &"
}

stop_beam_spark_topology() {
  ssh -o StrictHostKeyChecking=no `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME sh $ROOT/stream-bench.sh STOP_BEAM_SPARK_PROCESSING"
}

start_beam_flink_topology() {
  FLINK_VERSION=1.9.0
  FLINK_DIR="$ROOT/flink-$FLINK_VERSION"
  ssh -o StrictHostKeyChecking=no `whoami`@$ADMIN_HOST  "$FLINK_DIR/bin/flink run -c apache.beam.AdvertisingBeamStream $ROOT/apache-beam-validator/target/apache-beam-validator-0.1.0.jar --runner=FlinkRunner --beamConf=/home/schintap/streaming-benchmarks/conf/localConf.yaml > /dev/null 2>&1 &"
}

stop_beam_flink_topology() {
  ssh -o StrictHostKeyChecking=no `whoami`@$ADMIN_HOST "JAVA_HOME=$YJAVA_HOME sh stream-bench.sh STOP_BEAM_FLINK_PROCESSING  > /dev/null 2>&1 &"
}

start_redis() {
  # Using yum package manager to install available 3.x redis version on grid
  # Latest redis 6.x runs into issues with instruction sets on ylinux box
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "sudo yum -y install redis > /dev/null 2>&1 &"
  stop_redis
  # Disable protected mode for it to be accessible from across the nodes
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "redis-server --protected-mode no > /dev/null 2>&1 &"
  create_redis_campaigns
}

create_redis_campaigns() {
  # Cleanup redis and create new campaigns
  echo "Creating new campaigns in redis"
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "cd $ROOT/data && ROOT=$ROOT JAVA_CMD=$YJAVA_HOME/bin/java nohup lein run -n --configPath $ROOT/conf/localConf.yaml > /dev/null 2>&1 &"
}

stop_redis() {
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "nohup sh $ROOT/stream-bench.sh STOP_REDIS > /dev/null 2>&1 &"
}

start_load() {
  LOAD=`expr $1 / $PRODUCERS`
  # Create result dir
  create_redis_campaigns
  if [[ "STORM" = "$2" ]];
  then
    stop_storm_topology
    start_storm_topology
  elif [[ "FLINK" = "$2" ]];
  then
    stop_flink_topology
    start_flink_topology
  elif [[ "SPARK" = "$2" ]];
  then
    stop_spark_topology
    start_spark_topology
  elif [[ "BEAM_FLINK" = "$2" ]];
  then
    stop_beam_flink_topology
    start_beam_flink_topology
  elif [[ "BEAM_SPARK" = $2 ]];
  then
    stop_beam_spark_topology
    start_beam_spark_topology
  fi

  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "nohup mkdir -p ~/results/$2/$1 > /dev/null 2>&1 &"

  echo "Generating $1 events/sec"
  # Lets have five producers
  PRODS=`expr $PRODUCERS - 1`
  for i in $(seq 0 $PRODS);
  do
    echo "Starting producer on $i"
    #ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "LOAD=$LOAD nohup $ROOT/stream-bench.sh START_LOAD > /dev/null 2>&1 &"
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "LOAD=$LOAD ROOT=$ROOT JAVA_CMD=$YJAVA_HOME/bin/java nohup sh $ROOT/stream-bench.sh START_LOAD > /dev/null 2>&1 &"
  done
}

stop_load() {
  PRODS=`expr $PRODUCERS - 1`
  for i in $(seq 0 $PRODS);
  do
    echo "Stopping producer on $i"
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "ROOT=$ROOT JAVA_CMD=$YJAVA_HOME/bin/java sh $ROOT/stream-bench.sh STOP_LOAD"
  done
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "cd $ROOT/data && ROOT=$ROOT JAVA_CMD=$YJAVA_HOME/bin/java nohup lein run -g --configPath $ROOT/conf/localConf.yaml > /dev/null 2>&1 &"
}

init_setup() {
  echo "Commencing initial setup"
  rm -rf streaming-benchmarks*
  git clone git@git.ouroath.com:schintap/streaming-benchmarks.git -b bench_journey
  cd streaming-benchmarks
  sh stream-bench.sh SETUP
  cd ..
  zip -r  streaming-benchmarks.zip streaming-benchmarks
  for i in {0..9};
  do
    scp streaming-benchmarks.zip stbl1230n0$i.blue.ygrid.yahoo.com:~
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com 'nohup rm -rf $ROOT > /dev/null 2>&1 &'
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com 'nohup unzip -o streaming-benchmarks.zip > /dev/null 2>&1 &'
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com 'nohup yinst i yjava_jdk-8.0_8u252b09.5462251 > /dev/null 2>&1 &'
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com 'nohup yinst i yjava_maven > /dev/null 2>&1 &'
  done
  echo "Initial setup complete"
}

setup_configs() {
  for i in {0..9};
  do
    scp ./benchmarkConf.yaml stbl1230n0$i.blue.ygrid.yahoo.com:$ROOT/conf/
    scp ./localConf.yaml stbl1230n0$i.blue.ygrid.yahoo.com:$ROOT/conf/
    scp ./storm.yaml stbl1230n0$i.blue.ygrid.yahoo.com:$STORM_DIR/conf/storm.yaml
    scp ./zookeeper.properties stbl1230n0$i.blue.ygrid.yahoo.com:$KAFKA_DIR/config/
    scp ./flink_slaves stbl1230n0$i.blue.ygrid.yahoo.com:$FLINK_DIR/conf/
    scp ./flink-conf.yaml stbl1230n0$i.blue.ygrid.yahoo.com:$FLINK_DIR/conf/
    scp ./bashrc stbl1230n0$i.blue.ygrid.yahoo.com:~
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "mv bashrc ~/.bashrc"
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "mv $FLINK_DIR/conf/flink_slaves $FLINK_DIR/conf/slaves"
    scp ./spark_slaves stbl1230n0$i.blue.ygrid.yahoo.com:$SPARK_DIR/conf/
    ssh -o StrictHostKeyChecking=no -A `whoami`@stbl1230n0$i.blue.ygrid.yahoo.com "mv $SPARK_DIR/conf/spark_slaves $SPARK_DIR/conf/slaves"
    scp ./spark-env.sh stbl1230n0$i.blue.ygrid.yahoo.com:$SPARK_DIR/conf/
  done
}

stop_and_clean() {
  # Stop load first
  stop_load
  if [[ "STORM" = "$2" ]];
  then
    # wait for few minutes for storm to catch up may be 30 seconds
    sleep 120
    echo "Stopping Storm topology and clean up"
    stop_storm_topology
  elif [[ "FLINK" = "$2" ]];
  then
    sleep 120
    echo "Stopping Flink topology and clean up"
    stop_flink_topology
  elif [[ "SPARK" = "$2" ]];
  then
    sleep 120
    echo "Stopping Spark topology and clean up"
    stop_spark_topology
  elif [[ "BEAM_FLINK" = "$2" ]];
  then
    sleep 120
    echo "Stopping Beam Flink topology and clean up"
    stop_beam_flink_topology
  elif [[ "BEAM_SPARK" = "$2" ]];
  then
    sleep 120
    echo "Stopping Beam Spark topology and clean up"
    stop_beam_spark_topology
  fi

  # Wait for messages to clean up retention period expiry 15 min retention
  # This way we dont have to stop kafka hosts and restart them
  sleep 900

  # move results for graphs seen/updated to results/s<framework>/load/seen,updated
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "nohup mv $ROOT/data/seen.txt ~/results/$2/$1/  > /dev/null 2>&1 &"
  ssh -o StrictHostKeyChecking=no -A `whoami`@$ADMIN_HOST "nohup mv $ROOT/data/updated.txt ~/results/$2/$1/ > /dev/null 2>&1 &"
}

run() {
  OP=$1
  if [[ "SETUP" = "$OP" ]];
  then
    echo "Setup initialized"
    #init_setup
    #setup_configs
    #setup_lein
    #setup_zookeeper_quorum
    #setup_kafka_instances
    #start_redis
    #start_zookeeper_quorum
    #start_kafka_instances
    start_flink_cluster
    start_beam_flink_topology
    stop_beam_flink_topology
    stop_flink_cluster
  elif [[ "STOP_LOAD" = "$OP" ]];
  then
    echo "Stopping Load"
    stop_load
  elif [[ "SETUP_CONFIGS" = "$OP" ]];
  then
    setup_configs
  elif [[ "RUN_STORM_SUITE" = "$OP" ]];
  then
    echo "Running storm benchmark suite..."
    start_storm_cluster
    start_load 50000 STORM
    sleep $TEST_TIME
    stop_and_clean 50000 STORM
    start_load 70000 STORM
    sleep $TEST_TIME
    stop_and_clean 70000 STORM
    start_load 90000 STORM
    sleep $TEST_TIME
    stop_and_clean 90000 STORM
    start_load 110000 STORM
    sleep $TEST_TIME
    stop_and_clean STORM
    start_load 130000 STORM
    sleep $TEST_TIME
    stop_and_clean 130000 STORM
    start_load 150000 STORM
    sleep $TEST_TIME
    stop_and_clean 150000 STORM
    start_load 170000 STORM
    sleep $TEST_TIME
    stop_and_clean 170000 STORM
    stop_storm_cluster
    echo "Storm benchmark suite completed..."
  elif [[ "RUN_FLINK_SUITE" = "$OP" ]];
  then
    echo "Running Flink benchmark suite..."
    start_flink_cluster
    start_load 50000 FLINK
    sleep $TEST_TIME
    stop_and_clean 50000 FLINK
    start_load 70000 STORM
    sleep $TEST_TIME
    stop_and_clean 70000 FLINK
    start_load 90000 STORM
    sleep $TEST_TIME
    stop_and_clean 90000 FLINK
    start_load 110000 FLINK
    sleep $TEST_TIME
    stop_and_clean FLINK
    start_load 130000 FLINK
    sleep $TEST_TIME
    stop_and_clean 130000 FLINK
    start_load 150000 FLINK
    sleep $TEST_TIME
    stop_and_clean 150000 FLINK
    start_load 170000 FLINK
    sleep $TEST_TIME
    stop_and_clean 170000 FLINK
    stop_flink_cluster
  elif [[ "RUN_SPARK_SUITE" = "$OP" ]];
  then
    echo "Running Spark benchmark suite..."
    start_spark_cluster
    start_load 50000 SPARK
    sleep $TEST_TIME
    stop_and_clean 50000 SPARK
    start_load 70000 SPARK
    sleep $TEST_TIME
    stop_and_clean 70000 SPARK
    start_load 90000 SPARK
    sleep $TEST_TIME
    stop_and_clean 90000 SPARK
    start_load 110000 SPARK
    sleep $TEST_TIME
    stop_and_clean 110000 SPARK
    start_load 130000 SPARK
    sleep $TEST_TIME
    stop_and_clean 130000 SPARK
    start_load 150000 SPARK
    sleep $TEST_TIME
    stop_and_clean 150000 SPARK
    start_load 170000 SPARK
    sleep $TEST_TIME
    stop_and_clean 170000 SPARK
    stop_spark_cluster
  elif [[ "RUN_SPARK_BEAM_SUITE" = "$OP" ]];
  then
    echo "Running Spark benchmark suite..."
    start_spark_cluster BEAM
    start_load 50000 BEAM_SPARK
    sleep $TEST_TIME
    stop_and_clean 50000 BEAM_SPARK
    start_load 70000 BEAM_SPARK
    sleep $TEST_TIME
    stop_and_clean 70000 BEAM_SPARK
    start_load 90000 BEAM_SPARK
    sleep $TEST_TIME
    stop_and_clean 90000 BEAM_SPARK
    start_load 110000 BEAM_SPARK
    sleep $TEST_TIME
    stop_and_clean BEAM_SPARK
    start_load 130000 BEAM_SPARK
    sleep $TEST_TIME
    stop_and_clean 130000 BEAM_SPARK
    start_load 150000 BEAM_SPARK
    sleep $TEST_TIME
    stop_and_clean 150000 BEAM_SPARK
    start_load 170000 BEAM_SPARK
    sleep $TEST_TIME
    stop_and_clean 170000 BEAM_SPARK
    stop_spark_cluster
  elif [[ "RUN_FLINK_BEAM_SUITE" = "$OP" ]];
  then
   echo "Running Beam Flink benchmark suite..."
   start_flink_cluster
   start_load 50000 BEAM_FLINK
   sleep $TEST_TIME
   stop_and_clean 50000 BEAM_FLINK
   start_load 70000 BEAM_FLINK
   sleep $TEST_TIME
   stop_and_clean 70000 BEAM_FLINK
   start_load 90000 BEAM_FLINK
   sleep $TEST_TIME
   stop_and_clean 90000 BEAM_FLINK
   start_load 110000 BEAM_FLINK
   sleep $TEST_TIME
   stop_and_clean BEAM_FLINK
   start_load 130000 BEAM_FLINK
   sleep $TEST_TIME
   stop_and_clean 130000 BEAM_FLINK
   start_load 150000 BEAM_FLINK
   sleep $TEST_TIME
   stop_and_clean 150000 BEAM_FLINK
   start_load 170000 BEAM_FLINK
   sleep $TEST_TIME
   stop_and_clean 170000 BEAM_FLINKs
   stop_flink_cluster
  elif [[ "STOP_ALL" = "$OP" ]];
  then
    stop_kafka_instances
    stop_zookeeper_quorum
    stop_redis
  else
    echo "Storm -> sh setup_cluster.sh RUN_STORM_SUITE"
    echo "Spark -> sh setup_cluster.sh RUN_STORM_SUITE"
    echo "Flink -> sh setup_cluster.sh RUN_STORM_SUITE"
    echo "Beam Flink -> sh setup_cluster.sh RUN_FLINK_BEAM_SUITE"
    echo "Beam Spark -> sh setup_cluster.sh RUN_SPARK_BEAM_SUITE"
  fi
}

if [[ $# -lt 1 ]];
then
  run "HELP"
else
  while [[ $# -gt 0 ]];
  do
    run "$1"
    shift
  done
fi


