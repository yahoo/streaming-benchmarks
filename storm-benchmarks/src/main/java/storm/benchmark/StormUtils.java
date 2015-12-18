package storm.benchmark;

import java.util.List;

public class StormUtils {
    private StormUtils(){}

    static String joinHosts(List<String> hosts, String port) {
        String joined = null;
        for(String s : hosts) {
            if(joined == null) {
                joined = "";
            }
            else {
                joined += ",";
            }

            joined += s + ":" + port;
        }
        return joined;
    }
}
