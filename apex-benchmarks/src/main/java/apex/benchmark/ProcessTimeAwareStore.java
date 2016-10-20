package apex.benchmark;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.dimensions.DimensionsEvent.Aggregate;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.contrib.dimensions.AppDataSingleSchemaDimensionStoreHDHT;

/**
 * This class is used for measure the benchmark for dimension computation.
 * 
 * The latency measured by dimension computation only count the time difference between generator and aggregation
 * But it doesn't count the time of aggregation unifier and store.
 * 
 * This class logged the max time of endWindow (all computation and store should be done before endWindow) 
 * and measure the latency as max(time of end window of same application window) - application window begin time - application window length.
 *
 */
public class ProcessTimeAwareStore extends AppDataSingleSchemaDimensionStoreHDHT
{
  private static final transient Logger logger = LoggerFactory.getLogger(ProcessTimeAwareStore.class);
  private static final long serialVersionUID = 3133508545508966286L;

  protected Map<Long, Map<String, Long>> bucketToKeyToUpdateTime = Maps.newHashMap();
  
  protected transient List<Aggregate> aggregateList = Lists.newArrayList();
  
  protected int logUpdateWindows = 1400; // 10 minutes + 100 seconds, ignore 20 window
  protected int windowCountForLog = 0;
  
  protected int loggedTimes = 0;
  protected int ignoreWindows = 20;
  
  @Override
  public void processEvent(Aggregate gae)
  {
    super.processEvent(gae);
    aggregateList.add(gae);
  }
  
  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    lastEndWindowTime = System.currentTimeMillis();
    logger.info("Store setup time: {}", System.currentTimeMillis());
  }
  
  private long lastEndWindowTime;
  
  @Override
  public void endWindow()
  {
    long endWindowStartTime = System.currentTimeMillis();
    
    super.endWindow();
    
    //update bucket update time;
    for(Aggregate aggregate : aggregateList) {
      updateUpdateTime(getKey(aggregate), getBucket(aggregate), System.currentTimeMillis());
    }
    
    aggregateList.clear();
    if(++windowCountForLog == logUpdateWindows){
      //logUpdateTime();
      logger.info("Logging Times: {}", loggedTimes);
      logFinalLatencies();
      logger.info("Logging Times: {}", loggedTimes);
      windowCountForLog = 0;
      ++loggedTimes;
    }
   
    //window set to 200
    long windowPeriod = System.currentTimeMillis() - lastEndWindowTime;
    if(windowPeriod  > 400) {
      logger.warn("Unexpected long window period: {}, endWindow executed time: {}", windowPeriod, System.currentTimeMillis() - endWindowStartTime);
    }
    lastEndWindowTime = System.currentTimeMillis();
  }
  
  protected String getKey(Aggregate aggregate)
  {
    return aggregate.getEventKey().getKey().getFieldsString()[0];
  }
  
  //all the buckets are same range, so just use begin or end time of the bucket to identify
  protected long getBucket(Aggregate aggregate)
  {
    return aggregate.getEventKey().getKey().getFieldsLong()[0];
  }
  
  protected void updateUpdateTime(String key, long bucket, long processTime)
  {
    Map<String, Long> keyToUpdateTime = bucketToKeyToUpdateTime.get(bucket);
    if(keyToUpdateTime == null) {
      keyToUpdateTime = Maps.newHashMap();
      bucketToKeyToUpdateTime.put(bucket, keyToUpdateTime);
    }
    keyToUpdateTime.put(key, processTime);
  }
  
  /**
   * for log, ignore first and last bucket as it maybe not complete
   */
  protected void logFinalLatencies()
  {
    List<Long> latencies = Lists.newArrayList();
    
    Set<Long> buckets = bucketToKeyToUpdateTime.keySet();
    List<Long> bucketList = Lists.newArrayList();
    bucketList.addAll(buckets);
    Collections.sort(bucketList);
    
    //remove beginning 10 windows
    if (bucketList.size() <= ignoreWindows) {
      return;
    }
    
    for(int i = 0; i<ignoreWindows; ++i) {
      bucketList.remove(0);
    } 
    bucketList.remove(bucketList.size() - 1);

    for(Long bucket : bucketList) {
      for(long updateTime : bucketToKeyToUpdateTime.get(bucket).values()) {
        //10000 is 10 seconds, the length of the window
        latencies.add(updateTime - bucket - 10000);
      }
    }
    
    Collections.sort(latencies);
    
    outputLatencyDetail(latencies);
    outputGroupByCount(latencies);
  }
  
  protected static void outputLatencyDetail(List<Long> latencies)
  {
    StringBuilder sb = new StringBuilder();
    for (long latency : latencies) {
      sb.append(latency).append("\n");
    }
    logger.info("latencies: \n {}", sb.toString());
  }
  
  /**
   * Each group have same count
   * @param latencies
   */
  protected static void outputGroupByCount(List<Long> latencies)
  {
    final int groups = 10;
    

    int totalCount = latencies.size();
    int step = totalCount / groups;
    int i=0;
    StringBuilder sb = new StringBuilder();
    for(; i<groups-1; ++i) {
      sb.append("" + i * 100/groups + " - " + (i+1) *100/groups + ": " + latencies.get(step * (i+1))).append("\n");
    }
    //last one
    sb.append("" + i * 100/groups + " - 100: " + latencies.get(latencies.size() - 1)).append("\n");
    
    logger.info("latency group by count: \n {}", sb.toString());
  }
}
