/**
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package apex.benchmark;

import java.util.*;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class JsonGenerator extends BaseOperator implements InputOperator
{
  private static final transient Logger logger = LoggerFactory.getLogger(JsonGenerator.class);
  
  public final transient DefaultOutputPort<JSONObject> out = new DefaultOutputPort<JSONObject>();

  private int adsIdx = 0;
  private int eventsIdx = 0;

  private String pageID = UUID.randomUUID().toString();
  private String userID = UUID.randomUUID().toString();
  private final String[] eventTypes = new String[]{"view", "click", "purchase"};

  public int getNumCampaigns()
  {
    return numCampaigns;
  }

  public void setNumCampaigns(int numCampaigns)
  {
    this.numCampaigns = numCampaigns;
  }

  public int getNumAdsPerCampaign()
  {
    return numAdsPerCampaign;
  }

  public void setNumAdsPerCampaign(int numAdsPerCampaign)
  {
    this.numAdsPerCampaign = numAdsPerCampaign;
  }

  private int numCampaigns = 100;
  private int numAdsPerCampaign = 10;

  private List<String> ads;
  private final Map<String, List<String>> campaigns;

  public JsonGenerator()
  {
    this.campaigns = generateCampaigns();
    this.ads = flattenCampaigns();
    dumpCampaigns();
  }
  
  public void dumpCampaigns()
  {
    StringBuilder sb = new StringBuilder();
    for (String campaign : campaigns.keySet()) {
      sb.append(campaign).append("\n");
    }
    logger.info("Campaigns: \n{}", sb.toString());
  }

  public Map<String, List<String>> getCampaigns()
  {
    return campaigns;
  }

  /**
   * Generate a single element
   */
  public JSONObject generateElement()
  {
    JSONObject jsonObject = new JSONObject();
    try  {

      jsonObject.put("user_id", userID);
      jsonObject.put("page_id", pageID);

      if (adsIdx == ads.size()) {
        adsIdx = 0;
      }

      if (eventsIdx == eventTypes.length) {
        eventsIdx = 0;
      }

      jsonObject.put("ad_id", ads.get(adsIdx++));
      jsonObject.put("ad_type", "banner78");
      jsonObject.put("event_type", eventTypes[eventsIdx++]);
      jsonObject.put("event_time", System.currentTimeMillis());
      jsonObject.put("ip_address", "1.2.3.4");
    }  catch ( JSONException json) {

    }

    return jsonObject;
  }

    /**
     * Generate a random list of ads and campaigns
     */
  private Map<String, List<String>> generateCampaigns()
  {
    Map<String, List<String>> adsByCampaign = new LinkedHashMap<>();
    for (int i = 0; i < numCampaigns; i++) {
      String campaign = UUID.randomUUID().toString();
      ArrayList<String> ads = new ArrayList<>();
      adsByCampaign.put(campaign, ads);
      for (int j = 0; j < numAdsPerCampaign; j++) {
        ads.add(UUID.randomUUID().toString());
      }
    }

    return adsByCampaign;
  }

    /**
     * Flatten into just ads
     */
  private List<String> flattenCampaigns()
  {
    // Flatten campaigns into simple list of ads
    List<String> ads = new ArrayList<>();
    for (Map.Entry<String, List<String>> entry : campaigns.entrySet()) {
      for (String ad : entry.getValue()) {
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

