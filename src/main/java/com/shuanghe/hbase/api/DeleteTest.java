package com.shuanghe.hbase.api;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-9
 * Time: 上午1:54
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class DeleteTest {
    public static void main(String[] args) {
        String name = "test";
        String family = "cf";

        Connection connection = null;
        Table table = null;
        TableName tableName = TableName.valueOf(name);

        try {
            connection = HBaseUtil.getCon();

            table = connection.getTable(tableName);

            //用行键来实例化Delete实例
            Delete delete = new Delete(Bytes.toBytes("row4"));

            delete.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"));

            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(connection, null, table);
        }
    }
}