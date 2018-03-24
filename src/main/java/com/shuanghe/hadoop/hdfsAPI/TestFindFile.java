package com.shuanghe.hadoop.hdfsAPI;

import com.shuanghe.hadoop.util.HdfsUtil;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 2018/01/10
 * Time: 18:16
 * To change this template use File | Settings | File Templates.
 * Description:找到某个文件的块数据在集群上的位置
 */
public class TestFindFile {
    private static Logger logger = Logger.getLogger(TestFindFile.class);

    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            testFindFile();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    private static void testFindFile() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        // 必须是一个具体的文件
        Path path = new Path("api/copyFromLocal.xml");
        // 文件状态
        FileStatus fileStatus = fs.getFileStatus(path);
        // 文件块
        BlockLocation[] blockLocations = fs.getFileBlockLocations(
                fileStatus, 0, fileStatus.getLen());
        int blockLen = blockLocations.length;
        logger.info("block Count:" + blockLen);

        for (int i = 0; i < blockLen; i++) {
            //主机名
            String[] hosts = blockLocations[i].getHosts();
            for (String s : hosts) {
                logger.info(String.format("主机名:%s", s));
            }
        }
    }
}