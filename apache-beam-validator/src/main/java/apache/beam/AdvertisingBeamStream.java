/**
 * Copyright 2020, Verizon Media Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apache.beam;

import avro.shaded.com.google.common.collect.ImmutableMap;
import benchmark.common.Utils;
import org.apache.beam.runners.flink.FlinkRunner;
import org.apache.beam.runners.spark.SparkRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.io.kafka.KafkaIO;

import java.util.List;
import java.util.Map;


/**
 * To Run:
 * SparkRunner:
 * "$SPARK_DIR/bin/spark-submit" --master spark://localhost:7077 --class apache.beam.AdvertisingBeamStream
 * ./apache-beam-validator/target/apache-beam-validator-0.1.0.jar "SparkRunner" "$CONF_FILE"
 *
 * FlinkRunner:
 * "$FLINK_DIR/bin/flink" run -c apache.beam.AdvertisingBeamStream ./apache-beam-validator/target/apache-beam-validator-0.1.0.jar "FlinkRunner" "$CONF_FILE"
 *
 */
public class AdvertisingBeamStream {

    private static final Logger LOG = LoggerFactory.getLogger(AdvertisingBeamStream.class);

    public static void main(final String[] args) throws Exception {


        if(args.length < 2) {
            System.out.println("Missing runner and config path");
            System.exit(1);
        }

        PipelineOptions options = PipelineOptionsFactory.create();
        if (args[0].equalsIgnoreCase("FlinkRunner")) {
            options.setRunner(FlinkRunner.class);
        } else {
            // Structured streaming runner works with 2.4.5 spark but not with 3.x version
            // Legacy DStream API works fine with 3.0
            options.setRunner(SparkRunner.class);
        }

        // Create the Pipeline object with the options we defined above.
        Pipeline p = Pipeline.create(options);

        Map commonConfig = Utils.findAndReadConfigFile(args[1], true);

        String kafkaBrokers = Utils.joinHosts((List<String>)commonConfig.get("kafka.brokers"),
                Integer.toString((Integer)commonConfig.get("kafka.port")));
        String kafkaTopic = (String)commonConfig.get("kafka.topic");


        p.apply(KafkaIO.<String, String>read()
                .withBootstrapServers(kafkaBrokers)
                .withTopic(kafkaTopic)
                .withKeyDeserializer(StringDeserializer.class)
                .withValueDeserializer(StringDeserializer.class)

                .updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object)"earliest"))

                // We're writing to a file, which does not support unbounded data sources. This line makes it bounded to
                // the first 5 records.
                // In reality, we would likely be writing to a data source that supports unbounded data, such as BigQuery.
                .withMaxNumRecords(5)

                .withoutMetadata() // PCollection<KV<Long, String>>
        )
                .apply(Values.<String>create())
                .apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
                    @ProcessElement
                    public void processElement(ProcessContext c) {
                        for (String word : c.element().split(",")) {
                            if (!word.isEmpty()) {
                                c.output(word);
                            }
                        }
                    }
                }))
                .apply(Count.<String>perElement())
                .apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
                    @Override
                    public String apply(KV<String, Long> input) {
                        return input.getKey() + ": " + input.getValue();
                    }
                }))
                .apply(TextIO.write().to("wordcounts"));

        p.run().waitUntilFinish();

    }
}