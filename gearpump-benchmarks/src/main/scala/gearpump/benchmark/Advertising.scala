package gearpump.benchmark

import akka.actor.ActorSystem
import benchmark.common.Utils
import benchmark.common.advertising.{CampaignProcessorCommon, RedisAdCampaignCache}
import io.gearpump.Message
import io.gearpump.partitioner.{Partitioner, UnicastPartitioner, HashPartitioner}
import io.gearpump.cluster.UserConfig
import io.gearpump.cluster.client.ClientContext
import io.gearpump.streaming.kafka.lib.StringMessageDecoder
import io.gearpump.streaming.{StreamApplication, Processor}
import io.gearpump.streaming.kafka.{KafkaSource, KafkaStorageFactory}
import io.gearpump.streaming.source.{DefaultTimeStampFilter, DataSourceProcessor}
import io.gearpump.streaming.task.{StartTime, Task, TaskContext}
import io.gearpump.util.{AkkaApp, Graph}
import org.json.JSONObject
import io.gearpump.util.Graph.Node
import scala.collection.JavaConverters._

object Advertising extends AkkaApp{

  def application(args: Array[String], system: ActorSystem) : StreamApplication = {
    implicit val actorSystem = system
    val commonConfig = Utils.findAndReadConfigFile(args(0), true).asInstanceOf[java.util.Map[String, Any]]

    val cores = commonConfig.get("process.cores").asInstanceOf[Int]
    val topic = commonConfig.get("kafka.topic").asInstanceOf[String]
    val partitions = commonConfig.get("kafka.partitions").asInstanceOf[Int]
    val redisHost = commonConfig.get("redis.host").asInstanceOf[String]

    val zookeeperHosts = commonConfig.get("zookeeper.servers").asInstanceOf[java.util.List[String]] match {
      case l: java.util.List[String] => l.asScala.toSeq
      case other => throw new ClassCastException(other + " not a List[String]")
    }
    val zookeeperPort = commonConfig.get("zookeeper.port").asInstanceOf[Int]
    val zookeeperConnect = zookeeperHosts.map(_ + ":" + zookeeperPort).mkString(",")

    val kafkaHosts = commonConfig.get("kafka.brokers").asInstanceOf[java.util.List[String]] match {
      case l: java.util.List[String] => l.asScala.toSeq
      case other => throw new ClassCastException(other + " not a List[String]")
    }
    val kafkaPort = commonConfig.get("kafka.port").asInstanceOf[Int]
    val brokerList = kafkaHosts.map(_ + ":" + kafkaPort).mkString(",")

    val parallel = Math.max(1, cores / 7)
    val gearConfig = UserConfig.empty.withString("redis.host", redisHost)
    val offsetStorageFactory = new KafkaStorageFactory(zookeeperConnect, brokerList)
    val source = new KafkaSource(topic, zookeeperConnect, offsetStorageFactory, new StringMessageDecoder, new DefaultTimeStampFilter)
    val sourceProcessor = DataSourceProcessor(source, partitions)
    val deserializer = Processor[DeserializeTask](parallel)
    val filter = Processor[EventFilterTask](parallel)
    val projection = Processor[EventProjectionTask](parallel)
    val join = Processor[RedisJoinTask](parallel)
    val campaign = Processor[CampaignProcessorTask](parallel * 2)
    val partitioner = new AdPartitioner

    val graph = Graph(sourceProcessor ~ new HashPartitioner ~> deserializer ~> filter ~> projection ~> join ~ partitioner ~> campaign)
    StreamApplication("Advertising", graph, gearConfig)
  }

  override def main(akkaConf: Advertising.Config, args: Array[String]): Unit = {
    val context = ClientContext(akkaConf)
    context.submit(application(args, context.system))
    context.close()
  }

  override def help: Unit = {}
}

class AdPartitioner extends UnicastPartitioner {
  override def getPartition(msg: Message, partitionNum: Int, currentPartitionId: Int): Int = {
    (msg.msg.asInstanceOf[(String, String, String)]._1.hashCode & Integer.MAX_VALUE) % partitionNum
  }
}

class DeserializeTask(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  override def onNext(msg : Message) : Unit = {
    val jsonObj = new JSONObject(msg.msg.asInstanceOf[String])
    val tuple = (
      jsonObj.getString("user_id"),
      jsonObj.getString("page_id"),
      jsonObj.getString("ad_id"),
      jsonObj.getString("ad_type"),
      jsonObj.getString("event_type"),
      jsonObj.getString("event_time"),
      jsonObj.getString("ip_address")
      )
    taskContext.output(Message(tuple, msg.timestamp))
  }
}

class EventFilterTask(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  override def onNext(msg: Message): Unit = {
    val tuple = msg.msg.asInstanceOf[(String, String, String, String, String, String, String)]
    if(tuple._5 == "view") {
      taskContext.output(msg)
    }
  }
}

class EventProjectionTask(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  override def onNext(msg: Message): Unit = {
    val tuple = msg.msg.asInstanceOf[(String, String, String, String, String, String, String)]
    taskContext.output(Message((tuple._3, tuple._6), msg.timestamp))
  }
}

class RedisJoinTask(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private val redisHost = conf.getString("redis.host").get
  private val redisAdCampaignCache = new RedisAdCampaignCache(redisHost)

  override def onStart(startTime : StartTime) : Unit = {
    redisAdCampaignCache.prepare()
  }

  override def onNext(msg: Message): Unit = {
    val (ad_id, event_time) = msg.msg.asInstanceOf[(String, String)]
    val campaign_id = redisAdCampaignCache.execute(ad_id)
    if(campaign_id != null) {
      val result = (campaign_id, ad_id, event_time)
      taskContext.output(Message(result, msg.timestamp))
    }
  }
}

class CampaignProcessorTask(taskContext : TaskContext, conf: UserConfig) extends Task(taskContext, conf) {
  private val redisHost = conf.getString("redis.host").get
  private val campaignProcessorCommon = new CampaignProcessorCommon(redisHost)

  override def onStart(startTime : StartTime) : Unit = {
    campaignProcessorCommon.prepare()
  }

  override def onNext(msg: Message): Unit = {
    val (campaign_id, _, event_time) = msg.msg.asInstanceOf[(String, String, String)]
    campaignProcessorCommon.execute(campaign_id, event_time)
  }
}
