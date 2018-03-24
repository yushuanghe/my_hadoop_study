package com.shuanghe.hadoop.hdfsAPI;

import com.shuanghe.hadoop.util.HdfsUtil;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 2018/01/10
 * Time: 17:32
 * To change this template use File | Settings | File Templates.
 * Description:列出目录中所有文件
 */
public class TestListStatus {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            listStatus();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    private static void listStatus() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        FileStatus[] fss = fs.listStatus(new Path("api"));
        Path[] listPath = FileUtil.stat2Paths(fss);
        for (Path p : listPath) {
            System.out.println(p);
        }
    }
}