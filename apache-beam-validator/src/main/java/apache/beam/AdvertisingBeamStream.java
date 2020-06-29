/**
 * Copyright 2020, Verizon Media Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apache.beam;

import avro.shaded.com.google.common.collect.ImmutableMap;
import benchmark.common.Utils;
import benchmark.common.advertising.CampaignProcessorCommon;
import benchmark.common.advertising.RedisAdCampaignCache;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.json.JSONObject;
import java.util.*;

/**
 * To Run:
 * SparkRunner:
 * "$SPARK_DIR/bin/spark-submit" --master spark://localhost:7077 --class apache.beam.AdvertisingBeamStream
 * ./apache-beam-validator/target/apache-beam-validator-0.1.0.jar --runner=SparkRunner "$CONF_FILE"
 *
 * SparkStructuredStreaming:
 * "$SPARK_DIR/bin/spark-submit" --master spark://localhost:7077 --class apache.beam.AdvertisingBeamStream
 * ./apache-beam-validator/target/apache-beam-validator-0.1.0.jar --runner=SparkStructuredStreamingRunner "$CONF_FILE"
 *
 * FlinkRunner:
 * "$FLINK_DIR/bin/flink" run -c apache.beam.AdvertisingBeamStream
 * ./apache-beam-validator/target/apache-beam-validator-0.1.0.jar --runner=FlinkRunner "$CONF_FILE"
 */
public class AdvertisingBeamStream {

    public interface AdvertisingBeamStreamOptions extends PipelineOptions {
        String getBeamConf();
        void setBeamConf(String value);
    }

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingBeamStream.class);

    public static void main(final String[] args) throws Exception {
        PipelineOptionsFactory.register(AdvertisingBeamStreamOptions.class);
        AdvertisingBeamStreamOptions options =
            PipelineOptionsFactory.fromArgs(args).as(AdvertisingBeamStreamOptions.class);

        Pipeline p = Pipeline.create(options);

        Map commonConfig = Utils.findAndReadConfigFile(options.getBeamConf(), true);

        String kafkaBrokers = Utils.joinHosts((List<String>)commonConfig.get("kafka.brokers"),
                Integer.toString((Integer)commonConfig.get("kafka.port")));
        String kafkaTopic = (String)commonConfig.get("kafka.topic");
        String redisServerHost = (String)commonConfig.get("redis.host");

        p.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(kafkaBrokers)
                .withTopic(kafkaTopic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)
                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))
                .withoutMetadata()
                )
                .apply(Values.<String>create())
                .apply("Parse Event", ParDo.of(new DoFn<String, Map<String,String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        JSONObject obj = new JSONObject(c.element());
                        Map<String, String> fields = new HashMap<String, String>();
                        fields.put("user_id", obj.getString("user_id"));
                        fields.put("page_id", obj.getString("page_id"));
                        fields.put("ad_id", obj.getString("ad_id"));
                        fields.put("ad_type", obj.getString("ad_type"));
                        fields.put("event_type", obj.getString("event_type"));
                        fields.put("event_time", obj.getString("event_time"));
                        fields.put("ip_address", obj.getString("ip_address"));
                        c.output(fields);
                    }
                }))
                .apply("Filter Event", ParDo.of(new DoFn<Map<String,String>, Map<String,String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        if (c.element().get("event_type")
                                .equalsIgnoreCase("view")) {
                            c.output(c.element());
                        }
                    }
                }))
                .apply("Redis Join Event", ParDo.of(new DoFn<Map<String,String>, List<String>>() {
                    transient RedisAdCampaignCache redisAdCampaignCache;

                    @StartBundle
                    public void startBundle() {
                        redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
                        this.redisAdCampaignCache.prepare();
                    }
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        String ad_id = c.element().get("ad_id");
                        String campaign_id = this.redisAdCampaignCache.execute(ad_id);
                        List<String> tuples = new ArrayList<>();
                        if (campaign_id != null) {
                            tuples.add(campaign_id);
                            tuples.add(ad_id);
                            tuples.add(c.element().get("event_time"));
                            c.output(tuples);
                        }
                    }
                }))
                .apply("Campaign Proccessing Event", ParDo.of(new DoFn<List<String>, String>() {
                    CampaignProcessorCommon campaignProcessorCommon;

                    @StartBundle
                    public void startBundle() {
                        this.campaignProcessorCommon = new CampaignProcessorCommon(redisServerHost);
                        this.campaignProcessorCommon.prepare();
                    }
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        this.campaignProcessorCommon.execute(c.element().get(0), c.element().get(2));
                    }
                }));
        p.run().waitUntilFinish();
    }
}
