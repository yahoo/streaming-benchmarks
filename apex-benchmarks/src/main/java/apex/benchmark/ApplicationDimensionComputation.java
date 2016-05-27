/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.net.URI;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.InputEvent;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.common.partitioner.StatelessPartitioner;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;
import com.datatorrent.contrib.dimensions.DimensionStoreHDHTNonEmptyQueryResultUnifier;
import com.datatorrent.contrib.hdht.tfile.TFileImpl;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import com.datatorrent.lib.counters.BasicCounters;
import com.datatorrent.lib.dimensions.DimensionsComputationFlexibleSingleSchemaPOJO;
import com.datatorrent.lib.io.PubSubWebSocketAppDataQuery;
import com.datatorrent.lib.io.PubSubWebSocketAppDataResult;
import com.datatorrent.lib.statistics.DimensionsComputationUnifierImpl;

/**
 * The application just include generator and dimensions computation
 * @author bright
 *
 */
@ApplicationAnnotation(name = ApplicationDimensionComputation.APP_NAME)
public class ApplicationDimensionComputation implements StreamingApplication
{
  public static final String APP_NAME = "DimensionComputation";
  public static final String DIMENSION_SCHEMA = "eventSchema.json";
  private static final transient Logger logger = LoggerFactory.getLogger(ApplicationDimensionComputation.class);
  
  protected static final int PARTITION_NUM = 8;
  
  protected String eventSchemaLocation = DIMENSION_SCHEMA;
  protected String PROP_STORE_PATH;
  
  protected int storePartitionCount = 1;
  
  public ApplicationDimensionComputation()
  {
    this(APP_NAME);
  }
  
  public ApplicationDimensionComputation(String appName)
  {
    PROP_STORE_PATH = "dt.application." + appName + ".operator.Store.fileStore.basePathPrefix";
  }
  
  @Override
  public void populateDAG(DAG dag, Configuration configuration) 
  {
    DimensionTupleGenerateOperator generateOperator = new DimensionTupleGenerateOperator();
    dag.addOperator("Generator", generateOperator);
    dag.setAttribute(generateOperator, Context.OperatorContext.PARTITIONER, new StatelessPartitioner<EventGenerator>(PARTITION_NUM));
    
    populateDimensionsDAG(dag, configuration, generateOperator.outputPort);
  }

  public void populateDimensionsDAG(DAG dag, Configuration conf, DefaultOutputPort<DimensionTuple> upstreamPort) 
  {
    final String eventSchema = SchemaUtils.jarResourceFileToString(eventSchemaLocation);
    
    // dimension
    DimensionsComputationFlexibleSingleSchemaPOJO dimensions = dag.addOperator("DimensionsComputation",
        DimensionsComputationFlexibleSingleSchemaPOJO.class);
    dag.setAttribute(dimensions, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    dag.setAttribute(dimensions, Context.OperatorContext.CHECKPOINT_WINDOW_COUNT, 10);

    // Set operator properties
    // key expression
    {
      Map<String, String> keyToExpression = Maps.newHashMap();
      keyToExpression.put("adId", "adId");
      keyToExpression.put("campaignId", "campaignId");
      keyToExpression.put("eventTime", "eventTime");
      keyToExpression.put("time", "eventTime");
      dimensions.setKeyToExpression(keyToExpression);
    }

    // aggregate expression
    {
      Map<String, String> valueToExpression = Maps.newHashMap();
      valueToExpression.put("clicks", "clicks");
      dimensions.setAggregateToExpression(valueToExpression);
    }

    // event schema
    dimensions.setConfigurationSchemaJSON(eventSchema);
    dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());

    dag.setUnifierAttribute(dimensions.output, OperatorContext.MEMORY_MB, 4096);
    dag.setAttribute(dimensions, Context.OperatorContext.APPLICATION_WINDOW_COUNT, 10);
    dag.setInputPortAttribute(dimensions.input, Context.PortContext.PARTITION_PARALLEL, true);
    
    // store
    AppDataSingleSchemaDimensionStoreHDHT store = createStore(dag, conf, eventSchema); 
    
    PubSubWebSocketAppDataQuery query = createQuery(dag, conf, store);


    // wsOut
    PubSubWebSocketAppDataResult wsOut = createQueryResult(dag, conf, store);

    dag.addStream("GenerateStream", upstreamPort, dimensions.input).setLocality(Locality.CONTAINER_LOCAL);
    dag.addStream("DimensionalStream", dimensions.output, store.input);
    dag.addStream("QueryResult", store.queryResult, wsOut.input);
  }
  
  
  protected AppDataSingleSchemaDimensionStoreHDHT createStore(DAG dag, Configuration conf,  String eventSchema)
  {
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", AppDataSingleSchemaDimensionStoreHDHT.class);
    store.setUpdateEnumValues(true);
    String basePath = Preconditions.checkNotNull(conf.get(PROP_STORE_PATH),
          "base path should be specified in the properties.xml");
    TFileImpl hdsFile = new TFileImpl.DTFileImpl();
    basePath += System.currentTimeMillis();
    hdsFile.setBasePath(basePath);

    store.setFileStore(hdsFile);
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
        new BasicCounters.LongAggregator<MutableLong>());
    store.setConfigurationSchemaJSON(eventSchema);
    
    if(storePartitionCount > 1)
    {
      store.setPartitionCount(storePartitionCount);
      store.setQueryResultUnifier(new DimensionStoreHDHTNonEmptyQueryResultUnifier());
    }
    return store;
  }
  
  
  protected String getQueryUriString(DAG dag, Configuration conf)
  {
    return ConfigUtil.getAppDataQueryPubSubUriString(dag, conf);
  }
  
  protected URI getQueryUri(DAG dag, Configuration conf)
  {
    return URI.create(getQueryUriString(dag, conf));
  }
  
  
  protected PubSubWebSocketAppDataQuery createQuery(DAG dag, Configuration conf, AppDataSingleSchemaDimensionStoreHDHT store)
  {
    PubSubWebSocketAppDataQuery query = new PubSubWebSocketAppDataQuery();
    URI queryUri = getQueryUri(dag, conf);
    logger.info("QueryUri: {}", queryUri);
    query.setUri(queryUri);
    store.setEmbeddableQueryInfoProvider(query);

    return query;
  }
  
  
  protected PubSubWebSocketAppDataResult createQueryResult(DAG dag, Configuration conf, AppDataSingleSchemaDimensionStoreHDHT store)
  {
    PubSubWebSocketAppDataResult wsOut = new PubSubWebSocketAppDataResult();
    URI queryUri = getQueryUri(dag, conf);
    wsOut.setUri(queryUri);
    dag.addOperator("QueryResult", wsOut);
    // Set remaining dag options
  
    dag.setAttribute(store, Context.OperatorContext.COUNTERS_AGGREGATOR,
        new BasicCounters.LongAggregator<MutableLong>());
    
    return wsOut;
  }
}
