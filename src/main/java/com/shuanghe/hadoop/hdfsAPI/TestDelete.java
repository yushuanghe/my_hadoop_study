package com.shuanghe.hadoop.hdfsAPI;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;

/**
 * Created by yushuanghe on 2017/02/13.
 */
public class TestDelete {
    public static void main(String[] args) {
        UserGroupInformation.createRemoteUser("yushuanghe").doAs(
                new PrivilegedAction<Object>() {
                    @Override
                    public Object run() {
                        try {
//                  delete();
                            deleteOnExit();
                        } catch (IOException | InterruptedException e) {
                            e.printStackTrace();
                        }

                        return null;
                    }
                }
        );
    }

    /**
     * 直接删除
     * 删除成功返回true，不存在返回false，不递归删除非空目录返回false
     *
     * @throws IOException
     */
    private static void delete() throws IOException {
        FileSystem fs = HdfsUtil.getFileSystem();
        boolean deleted = fs.delete(new Path("api/TestCreateNewFile1.txt"), true);
        System.out.println(deleted ? "删除成功" : "删除失败");

        deleted = fs.delete(new Path("api/mkdirs1"), false);
        System.out.println(deleted ? "删除成功" : "删除失败");

        /*
         * Directory is not empty
         * 不递归删除非空目录抛出异常
         */
        deleted = fs.delete(new Path("api/mkdirs2"), false);
        System.out.println(deleted ? "删除成功" : "删除失败");
        fs.close();
    }

    /**
     * 在文件close时删除
     *
     * @throws IOException
     */
    private static void deleteOnExit() throws IOException, InterruptedException {
        FileSystem fs = HdfsUtil.getFileSystem();
        boolean deleted = fs.delete(new Path("api/mkdirs1"), true);//true时删除目录，否则异常
        System.out.println("delete:" + (deleted ? "删除成功" : "删除失败"));

        deleted = fs.delete(new Path("api/mkdirs2"), true);
        System.out.println(deleted ? "删除成功" : "删除失败");

        deleted = fs.deleteOnExit(new Path("api/create2.txt"));//由于关闭文件系统，标记路径将被删除
        System.out.println("deleteOnExit:" + (deleted ? "删除成功" : "删除失败"));

        //System.in.read();
        Thread.sleep(10000);

        fs.close();
    }
}
