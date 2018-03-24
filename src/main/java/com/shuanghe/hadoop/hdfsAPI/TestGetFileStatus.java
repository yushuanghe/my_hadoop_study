package com.shuanghe.hadoop.hdfsAPI;

import com.shuanghe.hadoop.util.HdfsUtil;
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
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
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
        FileStatus status = fs.getFileStatus(new Path("api/copyFromLocal.xml"));
        System.out.println(status.isDirectory() ? "路径" : "文件");

        System.out.println(String.format("path:%s", status.getPath()));
        System.out.println(String.format("length:%s", status.getLen()));
        System.out.println(String.format("isdir:%s", status.isDirectory()));
        System.out.println(String.format("block_replication:%s", status.getReplication()));
        System.out.println(String.format("blocksize:%s", status.getBlockSize()));
        System.out.println(String.format("modification_time:%s", status.getModificationTime()));
        System.out.println(String.format("access_time:%s", status.getAccessTime()));
        System.out.println(String.format("permission:%s", status.getPermission()));
        System.out.println(String.format("owner:%s", status.getOwner()));
        System.out.println(String.format("group:%s", status.getGroup()));
        //System.out.println(String.format("symlink:%s", status.getSymlink()));

        fs.close();
    }
}
