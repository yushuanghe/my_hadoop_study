package com.shuanghe.hbase;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.NamespaceDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * HBaseAdmin类
 * Created by yushuanghe on 2017/02/16.
 */
public class TestHBaseAdmin {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseUtil.getHBaseConfiguration();
        HBaseAdmin hBaseAdmin = new HBaseAdmin(conf);
        try {
            testCreateTable(hBaseAdmin);
            testGetTableDescribe(hBaseAdmin);
//            testDeleteTable(hBaseAdmin);
        } finally {
            hBaseAdmin.close();
        }
    }

    /**
     * 创建表
     *
     * @param hBaseAdmin
     * @throws IOException
     */
    static void testCreateTable(HBaseAdmin hBaseAdmin) throws IOException {
        //创建namespace
//        hBaseAdmin.createNamespace(NamespaceDescriptor.create("dd").build());

        TableName tableName = TableName.valueOf("users");
        HTableDescriptor descriptor = new HTableDescriptor(tableName);
        if (!hBaseAdmin.tableExists(tableName)) {
            descriptor.addFamily(new HColumnDescriptor("f"));
            descriptor.setMaxFileSize(10000L);
            hBaseAdmin.createTable(descriptor);
            System.out.println("表创建成功！");
        } else {
            System.out.println("表已存在！");
        }
    }

    /***
     * 获取表信息
     *
     * @param hBaseAdmin
     * @throws IOException
     */
    static void testGetTableDescribe(HBaseAdmin hBaseAdmin) throws IOException {
        TableName name = TableName.valueOf("users");
        if (hBaseAdmin.tableExists(name)) {
            HTableDescriptor ht1 = hBaseAdmin.getTableDescriptor(name);
            System.out.println(ht1);
        }
    }

    /**
     * 删除表，需要先disable
     *
     * @param hBaseAdmin
     * @throws IOException
     */
    static void testDeleteTable(HBaseAdmin hBaseAdmin) throws IOException {
        TableName name = TableName.valueOf("users");

        if (hBaseAdmin.tableExists(name)) {
            if (hBaseAdmin.isTableEnabled(name)) {
                hBaseAdmin.disableTable(name);
            }
            //TableNotDisabledException: users
            hBaseAdmin.deleteTable(name);
            System.out.println("删除成功！");
        } else {
            System.out.println("表不存在！");
        }
    }
}
