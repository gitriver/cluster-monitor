package cn.com.systex.cluster.monitor;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.security.PrivilegedExceptionAction;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.hdfs.protocol.HdfsConstants;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


/**
 * 获取datanode信息 需要先设置dfs.permissions 或者设置用户 dfs.permissions.supergroup
 * 
 * @author ding
 *
 */
public class HdfsMonitor {
    private static final Logger LOG = LoggerFactory.getLogger(HdfsMonitor.class);
    public static int BUFFER_SIZE;
    public static FileSystem fs;
    public static Configuration conf;

    public static void printDatanodeInfo() throws IOException {
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        DatanodeInfo[] live = hdfs.getDataNodeStats();
        for (DatanodeInfo datanodeInfo : live) {
            System.out.println(datanodeInfo.getDatanodeReport());
        }
    }


    public static void printFsStatus() throws IOException {
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;
        FsStatus ds = hdfs.getStatus();
        long capacity = ds.getCapacity();
        long used = ds.getUsed();
        long remaining = ds.getRemaining();
        long presentCapacity = used + remaining;
        boolean mode = hdfs.setSafeMode(HdfsConstants.SafeModeAction.SAFEMODE_GET);
        if (mode) {
            System.out.println("Safe mode is ON");
        }
        System.out.println("Configured Capacity: " + capacity + " (" + StringUtils.byteDesc(capacity) + ")");
        System.out.println("Present Capacity: " + presentCapacity + " ("
                + StringUtils.byteDesc(presentCapacity) + ")");
        System.out.println("DFS Remaining: " + remaining + " (" + StringUtils.byteDesc(remaining) + ")");
        System.out.println("DFS Used: " + used + " (" + StringUtils.byteDesc(used) + ")");
        System.out.println("DFS Used%: " + StringUtils.formatPercent(used / (double) presentCapacity, 2));

        System.out.println("Under replicated blocks: " + hdfs.getUnderReplicatedBlocksCount());
        System.out.println("Blocks with corrupt replicas: " + hdfs.getCorruptBlocksCount());
        System.out.println("Missing blocks: " + hdfs.getMissingBlocksCount());
    }


    // 初始化环境
    public static void init(String file) {
        Properties props = getProperties(file);
        BUFFER_SIZE = Integer.parseInt(props.getProperty("file.buffer.size"));

        conf = new Configuration();
        conf.addResource(new Path(props.getProperty("hdfs.conf.file")));
        conf.set("fs.defaultFS", props.getProperty("fs.defaultFS"));
        try {
            String ugi = props.getProperty("fs.ugi");
            if (Strings.isNullOrEmpty(ugi))
                fs = FileSystem.get(conf);
            else {
                UserGroupInformation.createProxyUser(ugi, UserGroupInformation.getLoginUser()).doAs(
                    new PrivilegedExceptionAction<Void>() {
                        @Override
                        public Void run() throws Exception {
                            fs = FileSystem.get(conf);
                            return null;
                        }

                    });
            }
        }
        catch (Exception e) {
            LOG.error("初始化FileSytem对象异常: ", e.getMessage());
            e.printStackTrace();
        }
    }


    public static Properties getProperties(String propFile) {
        Properties props = new Properties();
        try {
            InputStream stream = HdfsMonitor.class.getClassLoader().getResourceAsStream(propFile);
            props.load(stream);
            stream.close();
        }
        catch (FileNotFoundException e) {
            LOG.error("解析数据库配置文件:No such file " + propFile);
        }
        catch (Exception e1) {
            e1.printStackTrace();
            LOG.error("解析数据库配置文件:" + e1.getMessage() + "," + propFile);
        }
        return props;
    }
}
