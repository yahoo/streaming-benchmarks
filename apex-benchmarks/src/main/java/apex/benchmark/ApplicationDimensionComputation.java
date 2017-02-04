/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;
import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.InputEvent;
import org.apache.commons.lang.mutable.MutableLong;
import org.apache.hadoop.conf.Configuration;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context;
import com.datatorrent.api.Context.DAGContext;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.Context.PortContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.StreamCodec;
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
import com.datatorrent.lib.stream.DevNull;
import com.datatorrent.netlet.util.Slice;

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
  
  protected static final int PARTITION_NUM = 20;
  
  protected String eventSchemaLocation = DIMENSION_SCHEMA;
  protected String PROP_STORE_PATH;
  
  protected int storePartitionCount = 4;
  
  protected boolean includeQuery = true;
  
  protected static final int STREAMING_WINDOW_SIZE_MILLIS = 200;
  
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

    // Set operator properties
    // key expression
    {
      Map<String, String> keyToExpression = Maps.newHashMap();
      keyToExpression.put("campaignId", DimensionTuple.CAMPAIGNID);
      keyToExpression.put("time", DimensionTuple.EVENTTIME);
      dimensions.setKeyToExpression(keyToExpression);
    }

    // aggregate expression
    {
      Map<String, String> valueToExpression = Maps.newHashMap();
      valueToExpression.put("clicks", DimensionTuple.CLICKS);
      valueToExpression.put("latency", DimensionTuple.LATENCY);
      
      dimensions.setAggregateToExpression(valueToExpression);
    }

    // event schema
    dimensions.setConfigurationSchemaJSON(eventSchema);
    dimensions.setUnifier(new DimensionsComputationUnifierImpl<InputEvent, Aggregate>());

    dag.setUnifierAttribute(dimensions.output, OperatorContext.MEMORY_MB, 10240);
    
    dag.setInputPortAttribute(dimensions.input, Context.PortContext.PARTITION_PARALLEL, true);
    
    // store
    AppDataSingleSchemaDimensionStoreHDHT store = createStore(dag, conf, eventSchema); 
    store.setCacheWindowDuration(10000 * 5 / STREAMING_WINDOW_SIZE_MILLIS);   //cache for 5 windows
    dag.addStream("GenerateStream", upstreamPort, dimensions.input).setLocality(Locality.CONTAINER_LOCAL);
    
    StoreStreamCodec codec = new StoreStreamCodec();
    dag.setInputPortAttribute(store.input, PortContext.STREAM_CODEC, codec);
    dag.addStream("DimensionalStream", dimensions.output, store.input);

    
    if (includeQuery) {
      createQuery(dag, conf, store);

      // wsOut
      PubSubWebSocketAppDataResult wsOut = createQueryResult(dag, conf, store);

      dag.addStream("QueryResult", store.queryResult, wsOut.input);
    } else {
      DevNull devNull = new DevNull();
      dag.addOperator("devNull", devNull);
      dag.addStream("QueryResult", store.queryResult, devNull.data);
    }
    
    dag.setAttribute(DAGContext.STREAMING_WINDOW_SIZE_MILLIS, STREAMING_WINDOW_SIZE_MILLIS);
  }
  
  protected static class StoreStreamCodec implements StreamCodec<Aggregate>, Serializable
  {
    private static final long serialVersionUID = -482870472621208905L;

    protected transient Kryo kryo;

    public StoreStreamCodec()
    {
      this.kryo = new Kryo();
      this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    }
    
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException
    {
      in.defaultReadObject();
      this.kryo = new Kryo();
      this.kryo.setClassLoader(Thread.currentThread().getContextClassLoader());
    }
    
    @Override
    public int getPartition(Aggregate aggregate)
    {
      return aggregate.getEventKey().getKey().getFieldsString()[0].hashCode();
    }

    @Override
    public Object fromByteArray(Slice fragment)
    {
      final Input input = new Input(fragment.buffer, fragment.offset, fragment.length);
      try {
        return kryo.readClassAndObject(input);
      } finally {
        input.close();
      }
    }

    @Override
    public Slice toByteArray(Aggregate o)
    {
      final Output output = new Output(32, -1);
      try {
        kryo.writeClassAndObject(output, o);
      } finally {
        output.close();
      }
      return new Slice(output.getBuffer(), 0, output.position());
    }
  }
  
  protected AppDataSingleSchemaDimensionStoreHDHT createStore(DAG dag, Configuration conf,  String eventSchema)
  {
    AppDataSingleSchemaDimensionStoreHDHT store = dag.addOperator("Store", ProcessTimeAwareStore.class);
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
    store.setPartitionCount(storePartitionCount);
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
