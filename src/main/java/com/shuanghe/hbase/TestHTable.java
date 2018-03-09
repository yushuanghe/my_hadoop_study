package com.shuanghe.hbase;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * put,get,delete
 * Created by yushuanghe on 2017/02/16.
 */
public class TestHTable {

    private static final byte[] family = Bytes.toBytes("cf");

    public static void main(String[] args) throws IOException {
        Connection connection = null;
        Table table = null;
        try {
            TableName tableName = TableName.valueOf("test");
            connection = HBaseUtil.getCon();
            table = connection.getTable(tableName);

            //testPut(table);
            testGet(table);
            //testDelete(table);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(connection, null, table);
        }
    }

    /**
     * 测试put
     *
     * @param table
     */
    static void testPut(Table table) throws IOException {
        //单个put
        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("11"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("dali"));
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

        List<Put> list = new ArrayList<>();
        list.add(put2);
        list.add(put3);
        list.add(put4);

        table.put(list);

        //检测put，条件成功才插入，要求rowkey是一样的
        Put put5 = new Put(Bytes.toBytes("row5"));
        put5.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("999"));
        table.checkAndPut(Bytes.toBytes("row5"), Bytes.toBytes("cf"),
                Bytes.toBytes("id"), Bytes.toBytes("11"), put5);

    }

    /**
     * 测试get
     *
     * @param table
     * @throws IOException
     */
    private static void testGet(Table table) throws IOException {
        Get get = new Get(Bytes.toBytes("row1"));
        Result result = table.get(get);

        String id = Bytes.toString(result.getValue(family, Bytes.toBytes("id")));
        int age = Integer.valueOf(Bytes.toString(result.getValue(family, Bytes.toBytes("age"))));
        String name = Bytes.toString(result.getValue(family, Bytes.toBytes("name")));
        String rowkey = Bytes.toString(result.getRow());
        System.out.println(String.format("id=%s,age=%s,name=%s,rowkey=%s", id, age, name, rowkey));
    }

    /**
     * 测试delete
     *
     * @param table
     * @throws IOException
     */
    private static void testDelete(Table table) throws IOException {
        Delete delete = new Delete(Bytes.toBytes("row4"));
        //只删除一个版本
        //Delete the latest version of the specified column.
        //delete.addColumn(family, Bytes.toBytes("id"));
        //delete.addColumn(family, Bytes.toBytes("name"));

        //直接删除列簇
        delete.addFamily(family);
        table.delete(delete);
    }
}
