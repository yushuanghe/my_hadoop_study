package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.security.PrivilegedAction;

/**
 * hdfs dfs -cat
 * 只能读取文本文件，读取其他文件可能乱码
 * Created by yushuanghe on 2017/02/13.
 */
public class TestOpen {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            open();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    /**
     * 只能读取文本文件
     *
     * @throws IOException
     */
    private static void open() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        InputStream is = fs.open(new Path("api/TestCreateNewFile2.txt"), 4);
        BufferedReader in = new BufferedReader(new InputStreamReader(is));
        String line = null;
        while ((line = in.readLine()) != null) {
            System.out.println(line);
        }
        IOUtils.closeStream(in);
        IOUtils.closeStream(is);
        //in.close();
        //is.close();
        fs.close();
    }
}
