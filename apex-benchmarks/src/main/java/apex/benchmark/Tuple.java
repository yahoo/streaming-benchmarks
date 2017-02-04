/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */

package apex.benchmark;

import java.io.Serializable;

public class Tuple implements Serializable
{
  public String adId;
  public String campaignId;

  public String event_time;
  public String clicks;

  public Tuple()
  {
  }

  public Tuple(String adId, String campaignId, String eventTime, String clicks)
  {
    this.adId = adId;
    this.campaignId = campaignId;
    this.event_time = eventTime;
    this.clicks = clicks;
  }
}

