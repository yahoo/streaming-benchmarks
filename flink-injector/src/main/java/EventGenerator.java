import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.logging.Logger;

import com.google.common.collect.Lists;
import redis.clients.jedis.Jedis;

public class EventGenerator {

    private final static Logger LOG = Logger.getLogger(EventGenerator.class.getName());

    private final static int numCampaigns = 10;
    private final static int viewCapacityPerWindow = 10;
    private final static int eventCount = 10 * 1000000;
    private final static int timeDivisor = 10000; // 10 seconds

    private final static String [] adTypes = new String[] {
            "banner",
            "modal",
            "sponsored-search",
            "mail",
            "mobile"
    };

    private final static String []  eventTypes = new String[] {
            "view",
            "click",
            "purchase"
    };

    // lists of randomly generated UUIDs
    private List<String> ads;
    private List<String> campaigns;
    private List<String> pages;
    private List<String> users;

    private Random randomGenerator = new Random(System.currentTimeMillis());

    private int campaignIdx = 0;

    /**
     *
     * @param redisHost URL of RedisDB
     */
    public EventGenerator(String redisHost) {

        users = makeIds(100);
        pages = makeIds(100);

        campaigns = makeIds(numCampaigns);

        // hookup to Redis and write campaigns
        Jedis jedis = new Jedis(redisHost);
        jedis.flushAll();

        LOG.info("Writing campaigns to Redis.");
        campaigns.forEach(campaing -> {
            jedis.sadd("campaigns", campaing);
        });

        ads = makeIds(numCampaigns * 10);

        // map ads to campaigns
        List<List<String>> campaignsAds = Lists.partition(ads, 10);
        campaignsAds.forEach(campaignAds -> {
            campaignAds.forEach(ad -> {
                jedis.set(ad, campaigns.get(campaignIdx));
            });

            campaignIdx++;
        });

        jedis.close();
    }

    /**
     * Make a new event as a JSON object with random data at given time.
     * @param t event creation time
     * @return JSON object
     */
    public String makeEventAt(long t) {
        // TODO: add event skew and late-by options

        int index = randomGenerator.nextInt(users.size());
        String userid = users.get(index);
        index = randomGenerator.nextInt(pages.size());
        String pageid = pages.get(index);
        index = randomGenerator.nextInt(adTypes.length);
        String adType = adTypes[index];
        index = randomGenerator.nextInt(eventTypes.length);
        String eventType = eventTypes[index];
        index = randomGenerator.nextInt(ads.size());
        String adid = ads.get(index);

        StringBuilder builder = new StringBuilder();
        builder.append("{\"user_id\": \"").append(userid)
                .append("\", \"page_id\": \"").append(pageid)
                .append("\", \"ad_id\": \"").append(adid)
                .append("\", \"ad_type\": \"").append(adType)
                .append("\", \"event_type\": \"").append(eventType)
                .append("\", \"event_time\": \"").append(t)
                .append("\", \"ip_address\": \"1.2.3.4\"}");

        return builder.toString();
    }

    private List<String> makeIds(long n) {
        ArrayList<String> list = new ArrayList<>();

        for (long i = 0; i < n; i++) {
            list.add(UUID.randomUUID().toString());
        }

        return list;
    }
}
