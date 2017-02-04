/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.io.Serializable;

public class DimensionTuple implements Serializable
{
  private static final long serialVersionUID = -4382320567154145354L;
  public static final String ADID = "adId";
  public static final String CAMPAIGNID = "campaignId";
  public static final String EVENTTIME = "eventTime";
  public static final String CLICKS = "clicks";
  public static final String LATENCY = "latency";

  public String adId;
  public String campaignId;

  public long eventTime;
  public long clicks = 1;
  
  protected static boolean autoGeneratorCampaignId = true;

  public static DimensionTuple fromTuple(Tuple tuple)
  {
    /**
     * The campaignId should not empty in normal case. 
     * This part of code is used for unit test where campainId will not populated. 
     * Choose a special campainId for easier identify if any problem
     */
    if(tuple.campaignId == null && autoGeneratorCampaignId) {
      tuple.campaignId = "1111111111111111111";
    }
    if (tuple.adId == null || tuple.campaignId == null || tuple.event_time == null) {
      return null;
    }
    try {
      return new DimensionTuple(tuple);
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public DimensionTuple()
  {
  }

  public DimensionTuple(Tuple tuple)
  {
    this(tuple.adId, tuple.campaignId, Long.valueOf(tuple.event_time), tuple.clicks == null ? 1L : Long.valueOf(tuple.clicks));
  }

  /**
   * following code are for Hard Coded Dimension Computation only
   */

  public DimensionTuple(String adId, String campaignId, long eventTime, long clicks)
  {
    this.adId = adId;
    this.campaignId = campaignId;
    this.eventTime = eventTime;
    this.clicks = clicks;
  }
  
  public long getLatency()
  {
    return System.currentTimeMillis() - eventTime;
  }
}
