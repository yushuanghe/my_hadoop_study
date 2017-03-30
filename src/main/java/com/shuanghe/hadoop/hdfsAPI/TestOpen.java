package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
    UserGroupInformation.createRemoteUser("hadoop").doAs(
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
    FileSystem fileSystem = HdfsUtil.getFileSystem();
    InputStream is = fileSystem.open(new Path("api/create1.txt"));
    BufferedReader in = new BufferedReader(new InputStreamReader(is));
    String line = null;
    while ((line = in.readLine()) != null) {
      System.out.println(line);
    }
    in.close();
    is.close();
    fileSystem.close();
  }
}
