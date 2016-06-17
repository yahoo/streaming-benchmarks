/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;

public class DimensionTupleGenerateOperator implements InputOperator
{
  public final transient DefaultOutputPort<DimensionTuple> outputPort = new DefaultOutputPort<>();
  
  private int batchSize = 10;
  private int batchSleepTime = 2;
  private int campaignSize = 1000000;
  
  private DimensionTupleGenerator tupleGenerator = new DimensionTupleGenerator();

  @Override
  public void emitTuples() {
    if(outputPort.isConnected())
    {
      for(int i=0; i<batchSize; ++i)
      {
        outputPort.emit(tupleGenerator.next());
      }
    }
    
    if(batchSleepTime > 0)
    {
      try
      {
        Thread.sleep(batchSleepTime);
      }
      catch(Exception e){}
    }
  }
  

  public DimensionTupleGenerator getTupleGenerator()
  {
    return tupleGenerator;
  }

  public void setTupleGenerator(DimensionTupleGenerator tupleGenerator)
  {
    this.tupleGenerator = tupleGenerator;
  }

  public int getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(int batchSize)
  {
    this.batchSize = batchSize;
  }

  public int getBatchSleepTime()
  {
    return batchSleepTime;
  }

  public void setBatchSleepTime(int batchSleepTime)
  {
    this.batchSleepTime = batchSleepTime;
  }


  public int getCampaignSize()
  {
    return campaignSize;
  }


  public void setCampaignSize(int campaignSize)
  {
    this.campaignSize = campaignSize;
  }


  @Override
  public void beginWindow(long arg0)
  {
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext arg0)
  {
    tupleGenerator.setCampaignSize(campaignSize);
  }

  @Override
  public void teardown()
  {
  }
}
