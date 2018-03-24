package com.shuanghe.hadoop.hdfsAPI;

import com.shuanghe.hadoop.util.HdfsUtil;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 2018/01/10
 * Time: 18:25
 * To change this template use File | Settings | File Templates.
 * Description:列出hdfs集群上所有节点名称
 */
public class TestGetDataNodeStatus {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            testGetDataNodeStatus();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    private static void testGetDataNodeStatus() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        DistributedFileSystem hdfs = (DistributedFileSystem) fs;

        DatanodeInfo[] dataNodeStats = hdfs.getDataNodeStats();

        for (int i = 0; i < dataNodeStats.length; i++) {
            System.out.println("DataNode_" + i + "_Name:" + dataNodeStats[i].getHostName());
        }
    }
}