/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

package apex.benchmark;

import benchmark.common.advertising.RedisAdCampaignCache;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;

public class RedisJoin extends BaseOperator
{
  private transient RedisAdCampaignCache redisAdCampaignCache;
  private String redisServerHost;

  public String getRedisServerHost()
  {
    return redisServerHost;
  }

  public void setRedisServerHost(String redisServerHost)
  {
    this.redisServerHost = redisServerHost;
  }

  public transient DefaultInputPort<Tuple> input = new DefaultInputPort<Tuple>()
  {
    @Override
    public void process(Tuple tuple)
    {
      String campaign_id = redisAdCampaignCache.execute(String.valueOf(tuple.adId));

      if (campaign_id == null || campaign_id.isEmpty()) {
        return;
      }

      tuple.campaignId = campaign_id;

      output.emit(tuple);
    }
  };

  public transient DefaultOutputPort<Tuple> output = new DefaultOutputPort();

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.redisAdCampaignCache = new RedisAdCampaignCache(redisServerHost);
    this.redisAdCampaignCache.prepare();
  }
}

