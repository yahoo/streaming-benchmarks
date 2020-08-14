package storm.benchmark;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutConfig;
import org.apache.storm.kafka.spout.trident.KafkaTridentSpoutOpaque;
import org.apache.storm.trident.Stream;
import org.apache.storm.trident.TridentTopology;
import org.apache.storm.trident.operation.BaseFilter;
import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import benchmark.common.Utils;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.advertising.RedisAdCampaignCache;
import java.util.Map;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.json.JSONObject;

import static org.apache.storm.kafka.spout.FirstPollOffsetStrategy.EARLIEST;

/**
 * This is a basic example of a Storm topology.
 */
public class AdvertisingTridentTopology {

    public static class DeserializeJSONEvent extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {

            // Tuple contains "topic", "partition", "offset", "key", and "value"
            // We extract the value for our processing
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

    public static class FilterEvent extends BaseFilter {
        @Override
        public boolean isKeep(TridentTuple tuple) {
            return tuple.getStringByField("event_type").equals("view");
        }
    }


    public static class ProjectEvent extends BaseFunction {

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            collector.emit(new Values(tuple.getStringByField("ad_id"),
                    tuple.getStringByField("event_time")));
        }
    }

    public static class RedisJoin extends BaseFunction {
        transient RedisAdCampaignCache redisAdCampaignCache;
        private String redisServerHost;

        public RedisJoin(String redisServerHost) {
            this.redisServerHost = redisServerHost;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            if (this.redisAdCampaignCache == null) {
                redisAdCampaignCache = new RedisAdCampaignCache(this.redisServerHost);
                this.redisAdCampaignCache.prepare();
            }
            String ad_id = tuple.getStringByField("ad_id_projected");
            String campaign_id = this.redisAdCampaignCache.execute(ad_id);
            if(campaign_id == null) {
                return;
            }
            collector.emit(new Values(campaign_id,
                    tuple.getStringByField("ad_id_projected"),
                    tuple.getStringByField("event_time_projected")));
        }
    }

    public static class CampaignProcessor extends BaseFunction {

        transient private CampaignProcessorCommon campaignProcessorCommon;
        private String redisServerHost;

        public CampaignProcessor(String redisServerHost) {
            this.redisServerHost = redisServerHost;
        }

        @Override
        public void execute(TridentTuple tuple, TridentCollector collector) {
            if(this.campaignProcessorCommon == null) {
                campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
                this.campaignProcessorCommon.prepare();
            }
            String campaign_id = tuple.getStringByField("campaign_id");
            String event_time = tuple.getStringByField("event_time_joined");

            this.campaignProcessorCommon.execute(campaign_id, event_time);
            collector.emit(new Values(campaign_id));
        }
    }

    private static KafkaTridentSpoutConfig<String, String> getKafkaSpoutConfig(String bootstrapServers,
                                                                        String kafkaTopic) {
        return KafkaTridentSpoutConfig.builder(bootstrapServers, kafkaTopic)
                .setProp(ConsumerConfig.GROUP_ID_CONFIG, "storm-benchmark")
                .setProp("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class)
                .setProp("value.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class)
                .setFirstPollOffsetStrategy(EARLIEST)
                .build();
    }

    public static void main(String[] args) throws Exception {
        Options opts = new Options();
        opts.addOption("conf", true, "Path to the config file.");

        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(opts, args);
        // TODO: Looks like jar submission for storm topology does not allow
        // -conf, need to fix this, for now just a work around
        String configPath = cmd.getOptionValue("conf");

        // Temporary work around
        try {
            if (configPath == null) {
                // Assuming the second arg has the conf file
                configPath = args[1];
            }
        } catch (Exception e) {
            System.out.println(e);
            System.exit(1);
        }
        Map commonConfig = Utils.findAndReadConfigFile(configPath, true);

        String kafkaBrokers = Utils.joinHosts((List<String>)commonConfig.get("kafka.brokers"),
                Integer.toString((Integer)commonConfig.get("kafka.port")));

        String redisServerHost = (String)commonConfig.get("redis.host");
        String kafkaTopic = (String)commonConfig.get("kafka.topic");
        int kafkaPartitions = ((Number)commonConfig.get("kafka.partitions")).intValue();
        int workers = ((Number)commonConfig.get("storm.workers")).intValue();
        int ackers = ((Number)commonConfig.get("storm.ackers")).intValue();
        int cores = ((Number)commonConfig.get("process.cores")).intValue();
        int parallel = Math.max(1, cores/7);

        TridentTopology topology = new TridentTopology();
        Stream spoutStream = topology.newStream("KafkaSpout",
                new KafkaTridentSpoutOpaque(getKafkaSpoutConfig(kafkaBrokers, kafkaTopic)))
                .shuffle()
                .parallelismHint(kafkaPartitions);

        spoutStream
                .each(new Fields("value"), new DeserializeJSONEvent(),
                        new Fields("user_id",
                                "page_id",
                                "ad_id",
                                "ad_type",
                                "event_type",
                                "event_time",
                                "ip_address")).name("Trident Deserialize Bolt")
                .shuffle()
                .parallelismHint(kafkaPartitions)
                .filter(new FilterEvent()).name("Trident Filter Bolt")
                .shuffle()
                .parallelismHint(kafkaPartitions)
                .each(new Fields("ad_id", "event_time"),
                        new ProjectEvent(),
                        new Fields("ad_id_projected", "event_time_projected")).name("Trident Projection Bolt")
                .shuffle()
                .parallelismHint(parallel)
                .each(new Fields("ad_id_projected", "event_time_projected"),
                        new RedisJoin(redisServerHost),
                        new Fields("campaign_id", "ad_id_joined", "event_time_joined")).name("Trident Redis Join Bolt")
                .groupBy(new Fields("campaign_id"))
                .toStream()
                .parallelismHint(parallel)
                .each(new Fields("campaign_id", "ad_id_joined", "event_time_joined"),
                        new CampaignProcessor(redisServerHost),
                        new Fields("campaign_id_complete"))
                .shuffle()
                .parallelismHint(parallel*4);

        Config conf = new Config();

        if (args != null && args.length > 0) {
            conf.setNumWorkers(workers);
            conf.setNumAckers(ackers);
            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, topology.build());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, topology.build());
            org.apache.storm.utils.Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}