package com.shuanghe.hbase.api;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-9
 * Time: 上午12:45
 * To change this template use File | Settings | File Templates.
 * Description:CreateTable
 */
public class CreateTable {
    public static void main(String[] args) {
        String name = "test";
        String family = "cf";

        Connection connection = null;
        Admin admin = null;

        try {
            connection = HBaseUtil.getCon();
            //表管理类
            admin = connection.getAdmin();

            TableName tableName = TableName.valueOf(name);
            HTableDescriptor tableDescriptor = new HTableDescriptor(tableName);

            if (!admin.tableExists(tableName)) {
                //表不存在
                HColumnDescriptor columnDescriptor = new HColumnDescriptor(family);
                columnDescriptor.setMaxVersions(3);
                tableDescriptor.addFamily(columnDescriptor);

                Path path = new Path("hdfs://<namenode>:<port>/user/<hadoop-user>/coprocessor.jar");
                //tableDescriptor.addCoprocessor(RegionObserverExample.class.getCanonicalName(), path,Coprocessor.PRIORITY_USER, null);

                //定义表名
                admin.createTable(tableDescriptor);
                System.out.println("表创建成功！");

            } else {
                System.out.println("表已存在！");
                testGetTableDescribe(admin, tableName);
            }

            //增加列族
            //HColumnDescriptor newColumn = new HColumnDescriptor("cf2");
            //admin.addColumn(tableName, newColumn);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(connection, admin, null);
        }
    }

    private static void testGetTableDescribe(Admin admin, TableName tableName) throws IOException {
        if (admin.tableExists(tableName)) {
            HTableDescriptor table = admin.getTableDescriptor(tableName);
            System.out.println(table);
        }
    }
}