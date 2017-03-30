package com.shuanghe.hbase;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * put,get,delete
 * Created by yushuanghe on 2017/02/16.
 */
public class TestHTable {

    private static final byte[] family = Bytes.toBytes("f");

    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseUtil.getHBaseConfiguration();
        HTable hTable = null;

        testUseHBaseConnectionPool(conf);
    }

    /**
     * HBase线程池
     *
     * @param conf
     * @throws IOException
     */
    static void testUseHBaseConnectionPool(Configuration conf) throws IOException {
        ExecutorService threads = Executors.newFixedThreadPool(2);
        HConnection pool = HConnectionManager.createConnection(conf);
        HTableInterface hTable = pool.getTable("users");

        try {
//            hTable = new HTable(conf, "users");

//            testPut(hTable);
            testGet(hTable);
//            testDelete(hTable);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                hTable.close();//每次关闭其实是放到pool中
                pool.close();//最终关闭
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 测试put
     *
     * @param hTable
     */
    static void testPut(HTableInterface hTable) throws IOException {
        //单个put
        Put put = new Put(Bytes.toBytes("row1"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("11"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("dali"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("age"), Bytes.toBytes("22"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("sex"), Bytes.toBytes("m"));
        put.add(Bytes.toBytes("f"), Bytes.toBytes("phone"), Bytes.toBytes("1111111"));

        hTable.put(put);

        //多个put
        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("2"));
        put2.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("dali2"));
        Put put3 = new Put(Bytes.toBytes("row3"));
        put3.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("3"));
        put3.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("dali3"));
        Put put4 = new Put(Bytes.toBytes("row4"));
        put4.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("5"));
        put4.add(Bytes.toBytes("f"), Bytes.toBytes("name"), Bytes.toBytes("dali5"));

        List<Put> list = new ArrayList<>();
        list.add(put2);
        list.add(put3);
        list.add(put4);

        hTable.put(list);

        //检测put，条件成功才插入，要求rowkey是一样的
        Put put5 = new Put(Bytes.toBytes("row5"));
        put5.add(Bytes.toBytes("f"), Bytes.toBytes("id"), Bytes.toBytes("999"));
        hTable.checkAndPut(Bytes.toBytes("row5"), Bytes.toBytes("f"),
                Bytes.toBytes("id"), Bytes.toBytes("11"), put5);

    }

    /**
     * 测试get
     *
     * @param hTable
     * @throws IOException
     */
    static void testGet(HTableInterface hTable) throws IOException {
        Get get = new Get(Bytes.toBytes("row1"));
        Result result = hTable.get(get);

        String id = Bytes.toString(result.getValue(family, Bytes.toBytes("id")));
        int age = Integer.valueOf(Bytes.toString(result.getValue(family, Bytes.toBytes("age"))));
        String name = Bytes.toString(result.getValue(family, Bytes.toBytes("name")));
        String rowkey = Bytes.toString(result.getRow());
        System.out.println(String.format("id=%s,age=%s,name=%s,rowkey=%s", id, age, name, rowkey));
    }

    /**
     * 测试delete
     *
     * @param hTable
     * @throws IOException
     */
    static void testDelete(HTableInterface hTable) throws IOException {
        Delete delete = new Delete(Bytes.toBytes("row4"));
        //只删除一个版本
        //Delete the latest version of the specified column.
        delete.deleteColumn(family, Bytes.toBytes("id"));
        delete.deleteColumn(family, Bytes.toBytes("name"));

        //直接删除列簇
//        delete.deleteFamily(family);
        hTable.delete(delete);
    }
}
