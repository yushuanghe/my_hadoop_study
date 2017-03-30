package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * hdfsAPI 创建空文件
 * Created by yushuanghe on 2017/02/13.
 */
public class TestCreateNewFile {
  public static void main(String[] args) throws IOException {
    /**
     * 如果参数：dfs.permissions.enabled设置为true（默认为true），而且运行程序的用户名和hdfs
     * 有权限的用户不是同一个，需要使用
     */
    UserGroupInformation.createRemoteUser("hadoop").doAs(
            new PrivilegedAction<Object>() {
              @Override
              public Object run() {
                try {
                  createAbsolutePath();
                  createRelativePath();

                } catch (IOException e) {
                  e.printStackTrace();
                }

                return null;
              }
            }
    );
  }

  /**
   * 指定绝对路径
   */
  private static void createAbsolutePath() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://192.168.236.128:8020");
    FileSystem fileSystem = FileSystem.get(conf);

    boolean created = fileSystem.createNewFile(new Path("/user/hadoop/api/TestCreateNewFile1.txt"));
    System.out.println(created ? "创建成功" : "创建失败");
    fileSystem.close();
  }

  /**
   * 指定相对路径
   */
  private static void createRelativePath() throws IOException {
    Configuration conf = new Configuration();
    conf.set("fs.defaultFS", "hdfs://192.168.236.128:8020");
    FileSystem fs = FileSystem.get(conf);

    boolean created = fs.createNewFile(new Path("api/TestCreateNewFile2.txt"));
    System.out.println(created ? "创建成功" : "创建失败");
    fs.close();
  }
}
