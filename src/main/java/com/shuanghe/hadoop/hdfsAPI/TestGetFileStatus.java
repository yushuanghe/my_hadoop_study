package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Created by yushuanghe on 2017/02/13.
 */
public class TestGetFileStatus {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("hadoop").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            getFileStatus();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    private static void getFileStatus() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        FileStatus status = fs.getFileStatus(new Path("httpfs/test.txt"));
        System.out.println(status.isDirectory() ? "路径" : "文件");
        System.out.println(status.getPermission());
        System.out.println(status.getAccessTime());
        System.out.println(status.getReplication());
        System.out.println(status.getOwner());
        System.out.println(status.getLen());
        System.out.println(status.getModificationTime());

        fs.close();
    }
}
