package com.shuanghe.hadoop.util;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Created by yushuanghe on 2017/02/14.
 */
public class HdfsUtil {
    public static boolean deleteFile(String str, Configuration conf) throws IOException {
        FileSystem fs = null;
        try {
            fs = FileSystem.get(conf);
            Path path = new Path(str);

            return fs.delete(path, true);
        } finally {
            fs.close();
        }
    }

    public static Configuration getConfiguration() {
        Configuration conf = new Configuration();
        //conf.set("fs.defaultFS", "hdfs://192.168.236.128:8020");
        return conf;
    }

    public static FileSystem getFileSystem() throws IOException {
        return getFileSystem(getConfiguration());
    }

    public static FileSystem getFileSystem(Configuration conf) throws IOException {
        return FileSystem.get(conf);
    }
}
