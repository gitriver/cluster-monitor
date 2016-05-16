package cn.com.systex.cluster.monitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;


/**
 * 参考：https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/
 * ResourceManagerRest.html
 * 
 * @author ding
 *
 */
public class YarnMonitor {
    private final String baseUrl;
    private static final Logger LOG = LoggerFactory.getLogger(YarnMonitor.class);


    public YarnMonitor(String url) {
        baseUrl = url;
    }


    public String get(String url) {
        HttpResponse<String> res;
        try {
            res = Unirest.get(url).asString();
        }
        catch (UnirestException e) {
            LOG.error("出现异常{}", e.getMessage());
            e.printStackTrace();
            return null;
        }
        return res.getBody();
    }


    public String getClusterInfo() {
        String url = baseUrl + "/ws/v1/cluster/info";
        String res = get(url);
        return res;
    }


    public String getScheduler() {
        String url = baseUrl + "/ws/v1/cluster/scheduler";
        String res = get(url);
        return res;
    }


    public String getMetrics() {
        String url = baseUrl + "/ws/v1/cluster/metrics";
        String res = get(url);
        return res;
    }


    public String getApps() {
        String url = baseUrl + "/ws/v1/cluster/apps";
        String res = get(url);
        return res;
    }

}
