package cn.com.systex;

import java.io.IOException;

import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;

import cn.com.systex.cluster.monitor.HdfsMonitor;


/**
 *
 */
public class HDFSMonitorTest {
    public static void main(String[] args) throws IOException {
        HdfsMonitor.init("hadoop.properties");
        DistributedFileSystem hdfs = (DistributedFileSystem) HdfsMonitor.fs;
        FsStatus ds = hdfs.getStatus();
        long capacity = ds.getCapacity();
        long used = ds.getUsed();
        long remaining = ds.getRemaining();
        long presentCapacity = used + remaining;

        DatanodeInfo[] live = hdfs.getDataNodeStats();
        for (DatanodeInfo datanodeInfo : live) {
            System.out.println(datanodeInfo.getDatanodeReport());
        }

        System.out.println(capacity);
        System.out.println(used);

        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();
        System.out.println("Configured Capacity     （单位字节）:" + hdfs.getRawCapacity());
        System.out.println("DFS Used     （单位字节）:" + hdfs.getRawUsed());
        System.out.println("DFS Remaining    （单位字节） :" + hdfs.getDiskStatus().getRemaining());
        System.out.println("DFS Used%      :更具上面的信息自己算百分比");
        System.out.println("DFS Remaining%     : 更具上面的信息自己算百分比");
        System.out.println("Node信息参考下方代码27行");
        System.out.println("Number of Under-Replicated Blocks     :" + hdfs.getUnderReplicatedBlocksCount());

        System.out.println("===========1111111111");

        for (DatanodeInfo datanodeInfo : dataNodeStats) {
            System.out.println(datanodeInfo.getName());
            System.out.println(datanodeInfo.getAdminState());
            System.out.println(datanodeInfo.getCapacity());
            System.out.println(datanodeInfo.getDfsUsed());
            System.out.println(datanodeInfo.getNonDfsUsed());
            System.out.println(datanodeInfo.getRemaining());
            System.out.println(datanodeInfo.getSoftwareVersion());
            System.out.println(datanodeInfo.getBlockPoolUsed());

            System.out.println("===========");
        }
    }

}
