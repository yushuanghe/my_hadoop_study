package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;


/**
 * Created by yushuanghe on 2017/02/13.
 */
public class HdfsUtil {
  public static Configuration getConfiguration() {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://192.168.236.128:8020");
    return conf;
  }

  public static FileSystem getFileSystem() throws IOException {
    return getFileSystem(getConfiguration());
  }

  public static FileSystem getFileSystem(Configuration conf) throws IOException {
    return FileSystem.get(conf);
  }
}
