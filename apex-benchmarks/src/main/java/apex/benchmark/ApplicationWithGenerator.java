/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.util.List;
import java.util.Map;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;

@ApplicationAnnotation(name = "ApplicationWithGenerator")
public class ApplicationWithGenerator implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
     // Create operators for each step
     // settings are applied by the platform using the config file.
    JsonGenerator eventGenerator = dag.addOperator("eventGenerator", new JsonGenerator());
    FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples());
    FilterFields filterFields = dag.addOperator("filterFields", new FilterFields());
    RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
    CampaignProcessor campaignProcessor = dag.addOperator("campaignProcessor", new CampaignProcessor());

    eventGenerator.setNumAdsPerCampaign(Integer.parseInt(configuration.get("numberOfAds")));
    eventGenerator.setNumCampaigns(Integer.parseInt(configuration.get("numberOfCampaigns")));
    setupRedis(eventGenerator.getCampaigns(), configuration.get("redis"));

    // Connect the Ports in the Operators
    dag.addStream("filterTuples", eventGenerator.out, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("redisJoin", filterFields.output, redisJoin.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("output", redisJoin.output, campaignProcessor.input);

    dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(redisJoin.input, Context.PortContext.PARTITION_PARALLEL, true);

    dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(8));
    dag.setAttribute(campaignProcessor, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<CampaignProcessor>(8));
  }

  private void setupRedis(Map<String, List<String>> campaigns, String redis)
  {
    RedisHelper redisHelper = new RedisHelper();
    redisHelper.init(redis);

    redisHelper.prepareRedis(campaigns);
  }

}
