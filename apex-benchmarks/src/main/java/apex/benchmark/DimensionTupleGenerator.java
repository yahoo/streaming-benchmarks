/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.util.Random;

public class DimensionTupleGenerator
{
  public static final int maxClicks = 1000;
  
  public static final String adIdPrefix = "ad";
  protected int adIdSize = 100;
  
  public static final String compaignIdPrefix = "campaign";
  protected int campaignSize = 1000;

  
  protected static final Random random = new Random();
  public DimensionTuple next()
  {
    return new DimensionTuple(randomAdId(), randomCampaignId(), System.currentTimeMillis(), random.nextInt(maxClicks));
  }
  
  public String randomCampaignId()
  {
    return compaignIdPrefix + random.nextInt(campaignSize);
  }
  
  public String randomAdId()
  {
    return adIdPrefix + random.nextInt(adIdSize);
  }

  public int getAdIdSize()
  {
    return adIdSize;
  }

  public void setAdIdSize(int adIdSize)
  {
    this.adIdSize = adIdSize;
  }

  public int getCampaignSize()
  {
    return campaignSize;
  }

  public void setCampaignSize(int campaignSize)
  {
    this.campaignSize = campaignSize;
  }
}
