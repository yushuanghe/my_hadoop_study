package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * hdfsAPI 向文件写内容
 * Created by yushuanghe on 2017/02/13.
 */
public class TestAppend {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
                            Configuration conf = new Configuration();
                            //conf.set("fs.defaultFS", "hdfs://192.168.236.128:8020");
                            FileSystem fs = FileSystem.get(conf);
                            Path path = new Path("api/TestCreateNewFile2.txt");

                            FSDataOutputStream out = fs.append(path);
                            out.write("大力出奇迹！\n".getBytes());
                            out.flush();
                            IOUtils.closeStream(out);
                            //out.close();
                            fs.close();

                        } catch (IOException e) {
                            e.printStackTrace();
                        }


                        return null;
                    }
                }
        );
    }
}
