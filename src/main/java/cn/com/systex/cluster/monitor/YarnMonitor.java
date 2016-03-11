package cn.com.systex.cluster.monitor;

import com.mashape.unirest.http.HttpResponse;
import com.mashape.unirest.http.Unirest;
import com.mashape.unirest.http.exceptions.UnirestException;

/**
 * 参考：https://hadoop.apache.org/docs/r2.6.0/hadoop-yarn/hadoop-yarn-site/ResourceManagerRest.html
 * @author ding
 *
 */
public class YarnMonitor {
	
	public static String getInfo(String url) {
		String _url = "http://" + url + "/ws/v1/cluster/info";
		HttpResponse<String> res;
		try {
			res = Unirest.get(_url).asString();
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return res.getBody();
	}
	
	public static String getScheduler(String url) {
		String _url = "http://" + url + "/ws/v1/cluster/scheduler";
		HttpResponse<String> res;
		try {
			res = Unirest.get(_url).asString();
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return res.getBody();
	}
	
	public static String getMetrics(String url) {
		String _url = "http://" + url + "/ws/v1/cluster/metrics";
		HttpResponse<String> res;
		try {
			res = Unirest.get(_url).asString();
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return res.getBody();
	}
	
	public static String getApps(String url) {
		String _url = "http://" + url + "/ws/v1/cluster/apps";
		HttpResponse<String> res;
		try {
			res = Unirest.get(_url).asString();
		} catch (UnirestException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		return res.getBody();
	}
	
}
