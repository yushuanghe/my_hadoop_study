package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.security.PrivilegedAction;

/**
 * 创建文件并且写入内容
 * Created by yushuanghe on 2017/02/13.
 */
public class TestCreate {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        create();
                        create2();

                        return null;
                    }
                }
        );
    }

    /**
     * 默认create
     */
    private static void create() {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://192.168.236.131:8020");
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            FSDataOutputStream out = fileSystem.create(new Path("api/create1.txt"));
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, "utf-8"), true);
            pw.println("我有大力出奇迹！");
            pw.println("那是必须的！");

            //IOUtils.copyBytes(InputStream in, OutputStream out, int buffSize, boolean close);

            IOUtils.closeStream(pw);
            IOUtils.closeStream(out);
            //pw.close();
            //out.close();
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 指定副本数为3
     */
    private static void create2() {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://192.168.236.131:8020");
        try {
            FileSystem fileSystem = FileSystem.get(conf);
            FSDataOutputStream out = fileSystem.create(new Path("api/create2.txt"), (short) 3);
            PrintWriter pw = new PrintWriter(new OutputStreamWriter(out, "utf-8"), true);
            pw.println("副本数为3的文件创建！");
            pw.println("那是必须的！");
            IOUtils.closeStream(pw);
            IOUtils.closeStream(out);
            //pw.close();
            //out.close();
            fileSystem.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
