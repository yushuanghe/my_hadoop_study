package com.shuanghe.hbase.api;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-9
 * Time: 上午1:37
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class Read {
    public static void main(String[] args) {
        String name = "test";
        String family = "cf";

        Connection connection = null;
        Table table = null;
        TableName tableName = TableName.valueOf(name);

        try {
            connection = HBaseUtil.getCon();

            table = connection.getTable(tableName);

            Get get = new Get(Bytes.toBytes("row1"));
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"));
            get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
            //get.addFamily(Bytes.toBytes(family));

            //执行Get返回结果
            Result result = table.get(get);

            String id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("id")));
            //int age = Integer.valueOf(Bytes.toString(result.getValue(family, Bytes.toBytes("age"))));
            String name2 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("name")));
            //String rowkey = Bytes.toString(result.getRow());
            //System.out.println(String.format("id=%s,age=%s,name=%s,rowkey=%s", id, age, name, rowkey));
            System.out.println(String.format("id=%s,name=%s", id, name2));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(connection, null, table);
        }
    }
}