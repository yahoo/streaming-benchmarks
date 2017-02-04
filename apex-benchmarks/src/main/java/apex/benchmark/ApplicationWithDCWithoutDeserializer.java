/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;

@ApplicationAnnotation(name = ApplicationWithDCWithoutDeserializer.APP_NAME)
public class ApplicationWithDCWithoutDeserializer extends ApplicationDimensionComputation
{
  public static final String APP_NAME = "AppWithDCWithoutDe";
  
  protected static final int PARTITION_NUM = 20;
  
  protected String redisServer;
  
  protected boolean includeRedisJoin = true;
  
  public ApplicationWithDCWithoutDeserializer()
  {
    super(APP_NAME);
  }

  @Override
  public void populateDAG(DAG dag, Configuration configuration)
  {
    redisServer = configuration.get("dt.application.AppWithDCWithoutDe.redisServer");
    
    DefaultOutputPort<DimensionTuple> upstreamOutput = populateUpstreamDAG(dag, configuration);

    //populateHardCodedDimensionsDAG(dag, configuration, generateOperator.outputPort);
    populateDimensionsDAG(dag, configuration, upstreamOutput);
  }

  public DefaultOutputPort<DimensionTuple> populateUpstreamDAG(DAG dag, Configuration configuration)
  {
    JsonGenerator eventGenerator = dag.addOperator("eventGenerator", new JsonGenerator());
    FilterTuples filterTuples = dag.addOperator("filterTuples", new FilterTuples());
    FilterFields filterFields = dag.addOperator("filterFields", new FilterFields());
    
    // Connect the Ports in the Operators
    dag.addStream("filterTuples", eventGenerator.out, filterTuples.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    dag.addStream("filterFields", filterTuples.output, filterFields.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
    
    TupleToDimensionTupleConverter converter = dag.addOperator("converter", new TupleToDimensionTupleConverter());
    
    if(includeRedisJoin) {
      RedisJoin redisJoin = dag.addOperator("redisJoin", new RedisJoin());
      dag.addStream("redisJoin", filterFields.output, redisJoin.input).setLocality(DAG.Locality.CONTAINER_LOCAL);
      dag.addStream("converter", redisJoin.output, converter.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);

      dag.setInputPortAttribute(redisJoin.input, Context.PortContext.PARTITION_PARALLEL, true);

      setupRedis(eventGenerator.getCampaigns());
    } else {
      dag.addStream("convert", filterFields.output, converter.inputPort).setLocality(DAG.Locality.CONTAINER_LOCAL);
    }
    

    dag.setInputPortAttribute(filterTuples.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(filterFields.input, Context.PortContext.PARTITION_PARALLEL, true);
    dag.setInputPortAttribute(converter.inputPort, Context.PortContext.PARTITION_PARALLEL, true);

    dag.setAttribute(eventGenerator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(PARTITION_NUM));

    return converter.outputPort;
  }
  
  private void setupRedis(Map<String, List<String>> campaigns)
  {
    RedisHelper redisHelper = new RedisHelper();
    redisHelper.init(redisServer);

    redisHelper.prepareRedis(campaigns);
  }

  public String getRedisServer()
  {
    return redisServer;
  }

  public void setRedisServer(String redisServer)
  {
    this.redisServer = redisServer;
  }
}
