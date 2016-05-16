package cn.com.systex;

import cn.com.systex.cluster.monitor.YarnMonitor;


/**
 *
 */
public class YarnMonitorTest {
    public static void main(String[] args) {
        YarnMonitor monitor = new YarnMonitor("http://10.201.26.111:8088/");
        System.out.println(monitor.getClusterInfo());
    }
}
