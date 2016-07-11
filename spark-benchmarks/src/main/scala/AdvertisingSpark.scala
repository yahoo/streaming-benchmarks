/*
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

// scalastyle:off println

package spark.benchmark

import java.util

import kafka.serializer.StringDecoder
import org.apache.spark.streaming
import org.apache.spark.streaming.{Milliseconds, StreamingContext}

import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.dstream
import org.apache.spark.SparkConf
import org.json.JSONObject
import org.sedis._
import redis.clients.jedis._
import scala.collection.Iterator
import org.apache.spark.rdd.RDD
import java.util.{UUID, LinkedHashMap}
import compat.Platform.currentTime
import benchmark.common.Utils
import scala.collection.JavaConverters._
import scala.collection.mutable.Buffer

object KafkaRedisAdvertisingStream {
  def main(args: Array[String]) {

    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]];
    val batchSize = commonConfig.get("spark.batchtime") match {
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
    
    // Create context with 2 second batch interval
    val sparkConf = new SparkConf().setAppName("KafkaRedisAdvertisingStream")
    val ssc = new StreamingContext(sparkConf, Milliseconds(batchSize))

    val kafkaHosts = commonConfig.get("kafka.brokers").asInstanceOf[java.util.List[String]] match {
      case l: java.util.List[String] => l.asScala.toSeq
      case other => throw new ClassCastException(other + " not a List[String]")
    }
    val kafkaPort = commonConfig.get("kafka.port") match {
      case n: Number => n.toString()
      case other => throw new ClassCastException(other + " not a Number")
    }

    // Create direct kafka stream with brokers and topics
    val topicsSet = Set(topic)
    val brokers = joinHosts(kafkaHosts, kafkaPort)
    val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers, "auto.offset.reset" -> "smallest")
    System.err.println(
      "Trying to connect to Kafka at " + brokers)
    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    //We can repartition to use more executors if desired
    //    val messages_repartitioned = messages.repartition(10)


    //take the second tuple of the implicit Tuple2 argument _, by calling the Tuple2 method ._2
    //The first tuple is the key, which we don't use in this benchmark
    val kafkaRawData = messages.map(_._2)

    //Parse the String as JSON
    val kafkaData = kafkaRawData.map(parseJson(_))

    //Filter the records if event type is "view"
    val filteredOnView = kafkaData.filter(_(4).equals("view"))

    //project the event, basically filter the fileds.
    val projected = filteredOnView.map(eventProjection(_))

    //Note that the Storm benchmark caches the results from Redis, we don't do that here yet
    val redisJoined = projected.mapPartitions(queryRedisTopLevel(_, redisHost), false)

    val campaign_timeStamp = redisJoined.map(campaignTime(_))
    //each record in the RDD: key:(campaign_id : String, window_time: Long),  Value: (ad_id : String)
    //DStream[((String,Long),String)]

    // since we're just counting use reduceByKey
    val totalEventsPerCampaignTime = campaign_timeStamp.mapValues(_ => 1).reduceByKey(_ + _)

    //DStream[((String,Long), Int)]
    //each record: key:(campaign_id, window_time),  Value: number of events

    //Repartition here if desired to use more or less executors
    //    val totalEventsPerCampaignTime_repartitioned = totalEventsPerCampaignTime.repartition(20)

    totalEventsPerCampaignTime.foreachRDD { rdd =>
      rdd.foreachPartition(writeRedisTopLevel(_, redisHost))
    }

    // Start the computation
    ssc.start
    ssc.awaitTermination
  }

  def joinHosts(hosts: Seq[String], port: String): String = {
    val joined = new StringBuilder();
    hosts.foreach({
      if (!joined.isEmpty) {
        joined.append(",");
      }

      joined.append(_).append(":").append(port);
    })
    return joined.toString();
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

  def writeRedisTopLevel(campaign_window_counts_Iterator: Iterator[((String, Long), Int)], redisHost: String) {
    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, 6379, 2000))

    campaign_window_counts_Iterator.foreach(campaign_window_counts => writeWindow(pool, campaign_window_counts))

    pool.underlying.getResource.close
  }

  private def writeWindow(pool: Pool, campaign_window_counts: ((String, Long), Int)) : String = {
    val campaign_window_pair = campaign_window_counts._1
    val campaign = campaign_window_pair._1
    val window_timestamp = campaign_window_pair._2.toString
    val window_seenCount = campaign_window_counts._2
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
