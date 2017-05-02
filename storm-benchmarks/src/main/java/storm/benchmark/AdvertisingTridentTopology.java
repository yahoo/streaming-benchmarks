package storm.benchmark;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.FailedException;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import benchmark.common.Utils;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.advertising.RedisAdCampaignCache;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;
import storm.trident.Stream;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.TridentOperationContext;
import storm.trident.tuple.TridentTuple;

import java.util.List;
import java.util.Map;
import java.util.UUID;

public class AdvertisingTridentTopology {
    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingTridentTopology.class);


    static class DeserializeFunction extends BaseFunction {

        static Fields outputFields = new Fields("user_id", "page_id", "ad_id", "ad_type", "event_type", "event_time", "ip_address");

        public void execute(TridentTuple tuple, TridentCollector collector) {
            JSONObject obj = new JSONObject(tuple.getString(0));
            collector.emit(new Values(obj.getString("user_id"),
                    obj.getString("page_id"),
                    obj.getString("ad_id"),
                    obj.getString("ad_type"),
                    obj.getString("event_type"),
                    obj.getString("event_time"),
                    obj.getString("ip_address")));
        }
    }

    static class EventFilter extends BaseFilter {
        public boolean isKeep(TridentTuple tuple) {
            if (tuple.getStringByField("event_type").equals("view")) {
                return true;
            } else {
                return false;
            }
        }
    }

    static class RedisJoinFunction extends BaseFunction {
        transient RedisAdCampaignCache redisAdCampaignCache;
        private String redisServerHost;

        static Fields outputFields = new Fields("campaign_id");

        RedisJoinFunction(String redisServerHost) {
            this.redisServerHost = redisServerHost;
        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
            this.redisAdCampaignCache.prepare();
        }

        public void execute(TridentTuple tuple, TridentCollector collector) {
            String ad_id = tuple.getStringByField("ad_id");
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if (campaign_id == null) {
                throw new FailedException();
            }
            collector.emit(new Values(campaign_id));
        }
    }


    static class CampaignProcessorFunction extends BaseFunction {
        transient private CampaignProcessorCommon campaignProcessorCommon;
        private String redisServerHost;

        CampaignProcessorFunction(String redisServerHost) {
            this.redisServerHost = redisServerHost;
        }

        @Override
        public void prepare(Map conf, TridentOperationContext context) {
            campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
            this.campaignProcessorCommon.prepare();
        }

        public void execute(TridentTuple tuple, TridentCollector collector) {
            String campaign_id = tuple.getStringByField("campaign_id");
            String event_time = tuple.getStringByField("event_time");
            this.campaignProcessorCommon.execute(campaign_id, event_time);
        }
    }

    public static void main(String[] args) throws Exception {

        Options opts = new Options();
        opts.addOption("conf", true, "Path to the config file.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);
        String configPath = cmd.getOptionValue("conf");
        Map commonConfig = Utils.findAndReadConfigFile(configPath, true);

        String zkServerHosts = StormUtils.joinHosts((List<String>) commonConfig.get("zookeeper.servers"),
                Integer.toString((Integer) commonConfig.get("zookeeper.port")));
        String redisServerHost = (String) commonConfig.get("redis.host");
        String kafkaTopic = (String) commonConfig.get("kafka.topic");
        int kafkaPartitions = ((Number) commonConfig.get("kafka.partitions")).intValue();
        int workers = ((Number) commonConfig.get("storm.workers")).intValue();
        int ackers = ((Number) commonConfig.get("storm.ackers")).intValue();
        int cores = ((Number) commonConfig.get("process.cores")).intValue();
        int parallel = Math.max(1, cores / 7);

        ZkHosts hosts = new ZkHosts(zkServerHosts);

        TridentKafkaConfig spoutConfig = new TridentKafkaConfig(hosts, kafkaTopic, UUID.randomUUID().toString());
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());

        TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(spoutConfig);

        TridentTopology topology = new TridentTopology();

        Stream stream = topology.newStream("benchmark", kafkaSpout);

        Fields projectFields = new Fields("ad_id", "event_time");

        stream
                .parallelismHint(kafkaPartitions)
                .shuffle()
                // deserialize
                .each(new Fields("str"), new DeserializeFunction(), DeserializeFunction.outputFields)
                // event filter
                .each(DeserializeFunction.outputFields, new EventFilter())
                // project
                .project(projectFields)
                // redis join
                .each(projectFields, new RedisJoinFunction(redisServerHost), RedisJoinFunction.outputFields)
                .parallelismHint(parallel * 4)
                .partitionBy(new Fields("campaign_id"))
                // campaign processor
                .each(new Fields("ad_id", "event_time", "campaign_id"), new CampaignProcessorFunction(redisServerHost), new Fields())
                .parallelismHint(parallel * 2);


        Config conf = new Config();

        if (args != null && args.length > 0) {
            conf.setNumWorkers(workers);
            conf.setNumAckers(ackers);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology.build());
        } else {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, topology.build());
            backtype.storm.utils.Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
