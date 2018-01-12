package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * 上传，下载
 * Created by yushuanghe on 2017/02/13.
 */
public class TestCopy {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            copyFromLocal();
                            copyToLocal();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    /**
     * 上传文件
     *
     * @throws IOException
     */
    private static void copyFromLocal() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        fs.copyFromLocalFile(new Path("file:///home/yushuanghe/.m2/settings.xml"),
                new Path("api/copyFromLocal.xml"));
        fs.close();
    }

    /**
     * 下载文件
     *
     * @throws IOException
     */
    private static void copyToLocal() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        fs.copyToLocalFile(new Path("api/copyFromLocal.xml"),
                new Path("local/settings.xml")
        );
        fs.close();
    }
}
