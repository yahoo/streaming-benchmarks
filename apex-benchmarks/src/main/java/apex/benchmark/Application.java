/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import org.apache.hadoop.conf.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.kafka.KafkaSinglePortStringInputOperator;

@ApplicationAnnotation(name = "Apex_Benchmark")
public class Application implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    // Create operators for each step
    // settings are applied by the platform using the config file.
    KafkaSinglePortStringInputOperator kafkaInput = dag.addOperator("kafkaInput", new KafkaSinglePortStringInputOperator());
    DeserializeJSON deserializeJSON = dag.addOperator("deserialize", new DeserializeJSON());
    FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples());
    FilterFields filterFields = dag.addOperator("filterFields", new FilterFields());
    RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
    CampaignProcessor campaignProcessor = dag.addOperator("campaignProcessor", new CampaignProcessor());

    // Connect the Ports in the Operators
    dag.addStream("deserialize", kafkaInput.outputPort, deserializeJSON.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterTuples", deserializeJSON.output, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("redisJoin", filterFields.output, redisJoin.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("output", redisJoin.output, campaignProcessor.input);

    dag.setInputPortAttribute(deserializeJSON.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(redisJoin.input, Context.PortContext.PARTITION_PARALLEL, true);
  }

  private static final Logger LOG = LoggerFactory.getLogger(Application.class);
}

