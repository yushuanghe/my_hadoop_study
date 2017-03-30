package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * 创建文件夹，权限默认755
 * Created by yushuanghe on 2017/02/13.
 */
public class TestMkdirs {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("hadoop").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            mkdirs();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    private static void mkdirs() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        boolean mkdirsed = fs.mkdirs(new Path("api/mkdirs1"));
        System.out.println(mkdirsed ? "创建成功" : "创建失败");
        fs.close();
    }
}
