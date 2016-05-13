package cn.com.systex.cluster.cm;
import com.cloudera.api.ClouderaManagerClientBuilder;
import com.cloudera.api.DataView;
import com.cloudera.api.model.ApiClusterList;
import com.cloudera.api.model.ApiService;
import com.cloudera.api.model.ApiServiceState;
import com.cloudera.api.v10.RootResourceV10;
import com.cloudera.api.v10.ServicesResourceV10;

public class ClouderaApi {
	private static RootResourceV10 apiRoot = null;

	private static ApiClusterList clusters = null;

	private static ServicesResourceV10 resource = null;

	static {
		apiRoot = new ClouderaManagerClientBuilder().withHost("10.201.26.111")
				.withUsernamePassword("admin", "admin").build().getRootV10();
		clusters = apiRoot.getClustersResource().readClusters(DataView.SUMMARY);
		resource = apiRoot.getClustersResource().getServicesResource(
				clusters.get(0).getName());
	}

	/**
	 * 重启整个集群服务
	 */
	public static void reStartCluster() {
		apiRoot.getClustersResource().restartCommand(clusters.get(0).getName());
	}

	/**
	 * 启动整个集群服务
	 */
	public static void startCluster() {
		apiRoot.getClustersResource().startCommand(clusters.get(0).getName());
	}

	/**
	 * 停止整个集群服务
	 */
	public static void stopCluster() {
		apiRoot.getClustersResource().stopCommand(clusters.get(0).getName());
	}

	/**
	 * 重启某个服务 比如:重启yarn,那么serviceName为yarn
	 */
	public static void reStartService(String serviceName) {
		resource.restartCommand(serviceName);
	}

	/**
	 * 启动某个服务
	 */
	public static void startService(String serviceName) {
		resource.startCommand(serviceName);

	}

	/**
	 * 停止某个服务
	 */
	public static void stopService(String serviceName) {
		resource.stopCommand(serviceName);
	}

	/**
	 * 监控服务状态
	 */
	public static String monitorService(String serviceName) {
		ApiService as = resource.readService(serviceName);
		ApiServiceState ass = as.getServiceState();
		return ass.name();
	}

	public static void main(String[] args) {
		ApiService as = resource.readService("hdfs");
		ApiServiceState ass = as.getServiceState(); //停止STOPPED
		System.out.println(ass.name());
	}
	
}
