/*
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

// scalastyle:off println

package streams.benchmarks

import java.util
import java.util.{Properties, UUID}

import benchmark.common.Utils
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{KStream, KStreamBuilder, Reducer}
import org.apache.kafka.streams.processor.TopologyBuilder
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig}
import org.json.JSONObject
import org.sedis._
import redis.clients.jedis._
import streams.benchmarks.KeyValueImplicits._
import streams.benchmarks.StringLongSerde._

import scala.collection.JavaConverters._
import scala.compat.Platform.currentTime


object AdvertisingKafkaStreams {
  type Int = java.lang.Integer

  def main(args: Array[String]) {
    val appId = "AdvertisingKafkaStreams"
    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]]
    val batchSize = commonConfig.get("kafka.committime") match {
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
      case l: java.util.List[String] => l.asScala
      case other => throw new ClassCastException(other + " not a List[String]")
    }
    val kafkaPort = commonConfig.get("kafka.port") match {
      case n: Number => n.toString
      case other => throw new ClassCastException(other + " not a Number")
    }
    val brokers = joinHosts(kafkaHosts, kafkaPort)

    val props = new Properties()
    props.put(StreamsConfig.APPLICATION_ID_CONFIG, appId)
    props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, batchSize.toString)
    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")
    props.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)
    props.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass.getName)

    implicit val stringSerde = Serdes.String

    val pool = new Pool(new JedisPool(new JedisPoolConfig(), redisHost, 6379, 2000))
    val streamBuilder = new KStreamBuilder
    val messages = streamBuilder.stream[String, String](TopologyBuilder.AutoOffsetReset.EARLIEST, topic)

    //We can repartition to use more executors if desired
    //    val messages_repartitioned = messages.repartition(10)

    //Parse the String as JSON
    val kafkaData: KStream[String, Array[String]] = messages.mapValues[Array[String]]((v: String) => parseJson(v))

    //Filter the records if event type is "view"
    val filteredOnView: KStream[String, Array[String]] = kafkaData.filter((_, arr) => arr(4).equals("view"))

    //project the event, basically filter the fields.
    val projected: KStream[String, Array[String]] = filteredOnView.mapValues[Array[String]](eventProjection)

    //Note that the Storm benchmark caches the results from Redis, we don't do that here yet
    val redisJoined: KStream[String, Array[String]] = projected.mapValues(queryRedisTopLevel(_, redisHost, pool))

    //each record in the RDD: key:(campaign_id : String, window_time: Long),  Value: (ad_id : String)
    //DStream[((String,Long),String)]
    val campaign_timeStamp: KStream[StringLongTuple, String] = redisJoined.map[StringLongTuple, String]((_, v) => {
      val t = campaignTime(v)
      (t._1, t._2)
    })

    // since we're just counting use reduceByKey
    val totalEventsPerCampaignTime = campaign_timeStamp
      .mapValues[Int](_ => 1)
      .groupByKey(StringLongSerde.serde, Serdes.Integer())
      .reduce(
      new Reducer[Int] {
        override def apply(v: Int, v1: Int): Int = v + v1
      }, appId + "Reduce")

    totalEventsPerCampaignTime.foreach((k, v) => writeWindow(pool, (k, v)))

    // Start the computation
    val kafkaStreams = new KafkaStreams(streamBuilder, props)
    sys.addShutdownHook({
      kafkaStreams.close()
      pool.underlying.getResource.close()
    })
    kafkaStreams.start()
  }

  def joinHosts(hosts: Seq[String], port: String): String = {
    val joined = new StringBuilder()
    hosts.foreach({
      if (joined.nonEmpty) {
        joined.append(",")
      }

      joined.append(_).append(":").append(port)
    })
    joined.toString()
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

  def queryRedisTopLevel(event: Array[String], redisHost: String, pool: Pool): Array[String] = {
    val ad_to_campaign = new util.HashMap[String, String]()
    queryRedis(pool, ad_to_campaign, event)
  }

  def queryRedis(pool: Pool, ad_to_campaign: util.HashMap[String, String], event: Array[String]): Array[String] = {
    val ad_id = event(0)
    val campaign_id_cache = ad_to_campaign.get(ad_id)
    if (campaign_id_cache == null) {
      pool.withJedisClient { client =>
        val campaign_id_temp = Dress.up(client).get(ad_id)
        if (campaign_id_temp.isDefined) {
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

  def campaignTime(event: Array[String]): (StringLongTuple, String) = {
    val time_divisor: Long = 10000L
    ((event(0), time_divisor * (event(2).toLong / time_divisor)), event(1))
    //Key: (campaign_id, window_time),  Value: ad_id
  }

  private def writeWindow(pool: Pool, campaign_window_counts: ((String, Long), Int)): String = {
    val campaign_window_pair = campaign_window_counts._1
    val campaign = campaign_window_pair._1
    val window_timestamp = campaign_window_pair._2.toString
    val window_seenCount = campaign_window_counts._2.toLong
    pool.withJedisClient { client =>

      val dressUp = Dress.up(client)
      var windowUUID = dressUp.hmget(campaign, window_timestamp).head
      if (windowUUID == null) {
        windowUUID = UUID.randomUUID().toString
        dressUp.hset(campaign, window_timestamp, windowUUID)
        var windowListUUID: String = dressUp.hmget(campaign, "windows").head
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
