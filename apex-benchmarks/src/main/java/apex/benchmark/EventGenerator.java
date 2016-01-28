/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.util.*;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class EventGenerator extends BaseOperator implements InputOperator
{

  public final transient DefaultOutputPort<String> out = new DefaultOutputPort<String>();

  private int adsIdx = 0;
  private int eventsIdx = 0;
  private StringBuilder sb = new StringBuilder();
  private String pageID = UUID.randomUUID().toString();
  private String userID = UUID.randomUUID().toString();
  private final String[] eventTypes = new String[]{"view", "click", "purchase"};

  private List<Integer> ads;
  private final Map<Integer, List<Integer>> campaigns;

  public EventGenerator()
  {
    this.campaigns = generateCampaigns();
    this.ads = flattenCampaigns();
  }

  public Map<Integer, List<Integer>> getCampaigns()
  {
    return campaigns;
  }

  /**
   * Generate a single element
   */

  public String generateElement()
  {
    if (adsIdx == ads.size()) {
      adsIdx = 0;
    }

    if (eventsIdx == eventTypes.length) {
      eventsIdx = 0;
    }

    sb.setLength(0);
    sb.append("{\"user_id\":\"");
    sb.append(pageID);
    sb.append("\",\"page_id\":\"");
    sb.append(userID);
    sb.append("\",\"ad_id\":\"");
    sb.append(ads.get(adsIdx++));
    sb.append("\",\"ad_type\":\"");
    sb.append("banner78"); // value is immediately discarded. The original generator would put a string with 38/5 = 7.6 chars. We put 8.
    sb.append("\",\"event_type\":\"");
    sb.append(eventTypes[eventsIdx++]);
    sb.append("\",\"event_time\":\"");
    sb.append(System.currentTimeMillis());
    sb.append("\",\"ip_address\":\"1.2.3.4\"}");

    return sb.toString();

  }

    /**
     * Generate a random list of ads and campaigns
     */
  private Map<Integer, List<Integer>> generateCampaigns()
  {
    int numCampaigns = 100;
    int numAdsPerCampaign = 10;
    Random random = new Random();
    Map<Integer, List<Integer>> adsByCampaign = new LinkedHashMap<>();
    for (int i = 0; i < numCampaigns; i++) {
      Integer campaign = random.nextInt();
      ArrayList<Integer> ads = new ArrayList<>();
      adsByCampaign.put(campaign, ads);
      for (int j = 0; j < numAdsPerCampaign; j++) {
        ads.add(random.nextInt());
      }
    }
    return adsByCampaign;
  }

    /**
     * Flatten into just ads
     */
  private List<Integer> flattenCampaigns()
  {
  // Flatten campaigns into simple list of ads
    List<Integer> ads = new ArrayList<>();
    for (Map.Entry<Integer, List<Integer>> entry : campaigns.entrySet()) {
      for (Integer ad : entry.getValue()) {
        ads.add(ad);
      }
    }
    return ads;
  }

  @Override
  public void emitTuples()
  {
    for (int index = 0; index < 100; ++index) {
      out.emit(generateElement());
    }
  }
}

