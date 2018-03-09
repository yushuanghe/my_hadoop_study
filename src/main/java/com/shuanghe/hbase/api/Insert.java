package com.shuanghe.hbase.api;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-9
 * Time: 上午1:03
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class Insert {
    public static void main(String[] args) {
        String name = "test";
        String family = "cf";

        Connection connection = null;
        Table table = null;
        TableName tableName = TableName.valueOf(name);

        try {
            connection = HBaseUtil.getCon();

            String rowkey = "row1";
            String qulifier = "name";
            String value = "dali";

            //建立表连接
            table = connection.getTable(tableName);

            //单个put
            Put put = new Put(Bytes.toBytes(rowkey));
            put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qulifier), Bytes.toBytes(value));

            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("11"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes("22"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("sex"), Bytes.toBytes("m"));
            put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("phone"), Bytes.toBytes("1111111"));

            table.put(put);

            //多个put
            Put put2 = new Put(Bytes.toBytes("row2"));
            put2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("2"));
            put2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("dali2"));
            Put put3 = new Put(Bytes.toBytes("row3"));
            put3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("3"));
            put3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("dali3"));
            Put put4 = new Put(Bytes.toBytes("row4"));
            put4.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("5"));
            put4.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("dali5"));

            List<Put> puts = Arrays.asList(put2, put3, put4);

            table.put(puts);

            //检测put，条件成功才插入，要求rowkey是一样的
            Put put5 = new Put(Bytes.toBytes("row5"));
            put5.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("999"));

            table.checkAndPut(Bytes.toBytes("row5"), Bytes.toBytes("cf"),
                    Bytes.toBytes("id"), Bytes.toBytes("999"), put5);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(connection, null, table);
        }
    }
}