package com.shuanghe.hbase.api;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-9
 * Time: 上午2:00
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class DropTable {
    public static void main(String[] args) {
        String name = "users";
        String family = "cf";

        Connection connection = null;
        Admin admin = null;
        TableName tableName = TableName.valueOf(name);

        try {
            connection = HBaseUtil.getCon();

            admin = connection.getAdmin();

            //首先禁用表
            admin.disableTable(tableName);
            //最后删除表
            admin.deleteTable(tableName);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(connection, admin, null);
        }
    }
}