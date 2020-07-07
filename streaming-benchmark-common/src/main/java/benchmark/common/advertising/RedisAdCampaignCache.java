/**
 * Copyright 2015, Yahoo Inc.
 * Licensed under the terms of the Apache License 2.0. Please see LICENSE file in the project root for terms.
 */
package benchmark.common.advertising;

import redis.clients.jedis.Jedis;

import java.util.HashMap;

public class RedisAdCampaignCache {
    private Jedis jedis;
    private HashMap<String, String> ad_to_campaign;

    public RedisAdCampaignCache(String redisServerHostname) {
        jedis = new Jedis(redisServerHostname);
    }

    public void prepare() {
        ad_to_campaign = new HashMap<String, String>();
    }

    public String execute(String ad_id) {
        String campaign_id = ad_to_campaign.get(ad_id);
        if(campaign_id == null) {
            campaign_id = jedis.get(ad_id);
            if(campaign_id == null) {
                return null;
            }
            else {
                ad_to_campaign.put(ad_id, campaign_id);
            }
        }
        return campaign_id;
    }

    public void close() {
        jedis.close();
    }
}
