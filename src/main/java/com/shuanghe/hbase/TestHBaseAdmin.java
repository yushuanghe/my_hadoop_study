package com.shuanghe.hbase;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * HBaseAdmin类
 * Created by yushuanghe on 2017/02/16.
 */
public class TestHBaseAdmin {
    public static void main(String[] args) {
        Connection connection = HBaseUtil.getCon();
        Admin admin = null;

        try {
            admin = connection.getAdmin();

            testCreateTable(admin);
            testGetTableDescribe(admin);
            //testDeleteTable(admin);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 创建表
     *
     * @param admin
     * @throws IOException
     */
    private static void testCreateTable(Admin admin) throws IOException {
        //创建namespace
        //admin.createNamespace(NamespaceDescriptor.create("dd").build());

        TableName tableName = TableName.valueOf("default:users");
        HTableDescriptor table = new HTableDescriptor(tableName);
        if (!admin.tableExists(tableName)) {
            HColumnDescriptor cols1 = new HColumnDescriptor("cf");
            cols1.setMaxVersions(3);
            table.addFamily(cols1);

            admin.createTable(table);
            System.out.println("表创建成功！");
        } else {
            System.out.println("表已存在！");
        }
    }

    /***
     * 获取表信息
     *
     * @param admin
     * @throws IOException
     */
    private static void testGetTableDescribe(Admin admin) throws IOException {
        TableName name = TableName.valueOf("users");
        if (admin.tableExists(name)) {
            HTableDescriptor table = admin.getTableDescriptor(name);
            System.out.println(table);
        }
    }

    /**
     * 删除表，需要先disable
     *
     * @param admin
     * @throws IOException
     */
    private static void testDeleteTable(Admin admin) throws IOException {
        TableName name = TableName.valueOf("users");

        if (admin.tableExists(name)) {
            if (admin.isTableEnabled(name)) {
                admin.disableTable(name);
            }
            //TableNotDisabledException: users
            admin.deleteTable(name);
            System.out.println("删除成功！");
        } else {
            System.out.println("表不存在！");
        }
    }
}
