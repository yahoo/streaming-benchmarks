/*
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

// scalastyle:off println

package spark.benchmark.structuredstreaming

import java.util

import org.apache.spark.sql.{DataFrame, Dataset, ForeachWriter, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.json.JSONObject
import org.sedis._
import redis.clients.jedis._
import java.util.UUID

import compat.Platform.currentTime
import benchmark.common.Utils

import scala.collection.Iterator
import scala.collection.JavaConverters._

/**
  * Spark Structured Streaming API - Both batch and continuous mode available
  */
object KafkaRedisStructuredStreamingAdvertisingStream {
  def main(args: Array[String]) {

    if (args.length < 2 &&
      !(args(1).equalsIgnoreCase("batch") ||
        args(1).equalsIgnoreCase("continuos"))) {
      println("Please follow the spark-submit convention: \n" + "" +
        "\"$SPARK_DIR/bin/spark-submit\" " +
        "--master spark://localhost:7077 " +
        "--class spark.benchmark.structuredstreaming.KafkaRedisStructuredStreamingAdvertisingStream " +
        "./spark-benchmarks/target/spark-benchmarks-0.1.0.jar \"$CONF_FILE\" \"$MODE\" &\n")
      println("MODE should be Batch or Continuous")
      System.exit(1)
    }
    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]];
    val mode = args(1)
    val batchTriggerTime = commonConfig.get("spark.batch.time") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val continuosTriggerTime = commonConfig.get("spark.continuous.time") match {
      case n: Number => n.longValue()
      case other => throw new ClassCastException(other + " not a Number")
    }
    val topic = commonConfig.get("kafka.topic") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }

    val redisHost = commonConfig.get("redis.host") match {
      case s: String => s
      case other => throw new ClassCastException(other + " not a String")
    }
    val kafkaHosts = commonConfig.get("kafka.brokers").asInstanceOf[java.util.List[String]] match {
      case l: java.util.List[String] => l.asScala.toSeq
      case other => throw new ClassCastException(other + " not a List[String]")
    }
    val kafkaPort = commonConfig.get("kafka.port") match {
      case n: Number => n.toString()
      case other => throw new ClassCastException(other + " not a Number")
    }

    // Create direct kafka stream with brokers and topics
    val brokers = joinHosts(kafkaHosts, kafkaPort)

    val spark = SparkSession
      .builder
      .appName("KafkaRedisStructuredStreamingAdvertisingStream")
      .getOrCreate()

    System.err.println(
      "Trying to connect to Kafka at " + brokers)

    import spark.implicits._

    val messages = spark
      .readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", brokers)
      .option("subscribe", topic)
      .load()

    // Deserializing the streaming data frame
    val df = messages
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    // The first tuple is the key, which we don't use in this benchmark
    // Extract the value
    val kafkaRawData = df.map(row => row.getString(1))

    //Parse the String as JSON
    val kafkaData = kafkaRawData.map(parseJson(_))

    //Filter the records if event type is "view"
    val filteredOnView = kafkaData.filter(_(4).equals("view"))

    //project the event, basically filter the fields.
    val projected = filteredOnView.map(eventProjection(_))

    //val campaign_timeStamp = redisJoined.map(campaignTime(_))


    // TODO: Structured Streaming code has to change
    // Need to figure out aggregation part
    val query = projected
      .writeStream
      .format("console")
//      .foreach { new ForeachWriter[Array[String]] {
//        var pool: Pool = _
//
//        override def open(partitionId: Long, epochId: Long): Boolean = {
//          true
//        }
//
//        override def process(events: Array[String]): Unit = {
//          if (pool == null) {
//            pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, 6379, 2000))
//          }
//          var ad_to_campaign = new util.HashMap[String, String]();
//          val redisJoined = queryRedis(pool, ad_to_campaign, events)
//
//          //each record: key:(campaign_id : String, window_time: Long),  Value: (ad_id : String)
//          val campaign_timeStamp = campaignTime(redisJoined)
//
//
//          //each record: key:(campaign_id, window_time),  Value: number of events
//          //val totalEventsPerCampaignTime = campaign_timeStamp.
//
//          //Repartition here if desired to use more or less executors
//          //    val totalEventsPerCampaignTime_repartitioned = totalEventsPerCampaignTime.repartition(20)
//
//          //writeWindow(pool, campaign_timeStamp)
//
//
//        }
//
//        override def close(errorOrNull: Throwable): Unit = {
//          if (pool != null) {
//            pool.underlying.getResource.close
//          }
//        }
//      }
//    }

    // TODO: Refactor for batch and continuos mode
    if (mode.equalsIgnoreCase("continuous")) {
      val queryContext = query
        .trigger(Trigger.Continuous(continuosTriggerTime))
        .start()
      queryContext.awaitTermination()
    } else {
      val queryContext = query
        .trigger(Trigger.ProcessingTime(batchTriggerTime))
        .start()
      queryContext.awaitTermination()
    }
  }

  // TODO: Move all utility methods to Utils
  def joinHosts(hosts: Seq[String], port: String): String = {
    val joined = new StringBuilder();
    hosts.foreach({
      if (!joined.isEmpty) {
        joined.append(",");
      }

      joined.append(_).append(":").append(port);
    })
    return joined.toString;
  }

  def parseJson(jsonString: String): Array[String] = {
    val parser = new JSONObject(jsonString)
    Array(
      parser.getString("user_id"),
      parser.getString("page_id"),
      parser.getString("ad_id"),
      parser.getString("ad_type"),
      parser.getString("event_type"),
      parser.getString("event_time"),
      parser.getString("ip_address"))
  }

  def eventProjection(event: Array[String]): Array[String] = {
    Array(
      event(2), //ad_id
      event(5)) //event_time
  }

  def queryRedisTopLevel(eventsIterator: Iterator[Array[String]], redisHost: String): Iterator[Array[String]] = {
    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, 6379, 2000))
    var ad_to_campaign = new util.HashMap[String, String]();
    val eventsIteratorMap = eventsIterator.map(event => queryRedis(pool, ad_to_campaign, event))
    pool.underlying.getResource.close
    return eventsIteratorMap
  }

  def queryRedis(pool: Pool, ad_to_campaign: util.HashMap[String, String], event: Array[String]): Array[String] = {
    val ad_id = event(0)
    val campaign_id_cache = ad_to_campaign.get(ad_id)
    if (campaign_id_cache==null) {
      pool.withJedisClient { client =>
        val campaign_id_temp = Dress.up(client).get(ad_id)
        if (campaign_id_temp != None) {
          val campaign_id = campaign_id_temp.get
          ad_to_campaign.put(ad_id, campaign_id)
          Array(campaign_id, event(0), event(1))
          //campaign_id, ad_id, event_time
        } else {
          Array("Campaign_ID not found in either cache nore Redis for the given ad_id!", event(0), event(1))
        }
      }
    } else {
      Array(campaign_id_cache, event(0), event(1))
    }
  }

  def campaignTime(event: Array[String]): ((String, Long), String) = {
    val time_divisor: Long = 10000L
    ((event(0),time_divisor * (event(2).toLong / time_divisor)), event(1))
    //Key: (campaign_id, window_time),  Value: ad_id
  }


  private def writeWindow(pool: Pool, campaign_window_counts: ((String, Long), String)) : String = {
    val campaign_window_pair = campaign_window_counts._1
    val campaign = campaign_window_pair._1
    val window_timestamp = campaign_window_pair._2.toString
    val window_seenCount = 1700
    pool.withJedisClient { client =>

      val dressUp = Dress.up(client)
      var windowUUID = dressUp.hmget(campaign, window_timestamp)(0)
      if (windowUUID == null) {
        windowUUID = UUID.randomUUID().toString
        dressUp.hset(campaign, window_timestamp, windowUUID)
        var windowListUUID: String = dressUp.hmget(campaign, "windows")(0)
        if (windowListUUID == null) {
          windowListUUID = UUID.randomUUID.toString
          dressUp.hset(campaign, "windows", windowListUUID)
        }
        dressUp.lpush(windowListUUID, window_timestamp)
      }
      dressUp.hincrBy(windowUUID, "seen_count", window_seenCount)
      dressUp.hset(windowUUID, "time_updated", currentTime.toString)
      return window_seenCount.toString
    }

  }
}
