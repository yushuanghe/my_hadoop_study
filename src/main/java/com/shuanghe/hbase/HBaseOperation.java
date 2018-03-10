package com.shuanghe.hbase;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-8
 * Time: 下午11:41
 * To change this template use File | Settings | File Templates.
 * Description:CRUD Operations
 */
public class HBaseOperation {
    public static void main(String[] args) {
        Connection connection = HBaseUtil.getCon();
        TableName tableName = TableName.valueOf("test");
        try {
            Table table = connection.getTable(tableName);

            //get(table);
            //put(table);
            //delete(table);
            scan(table);

            //table.close();
            IOUtils.closeStream(table);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void get(Table table) throws IOException {
        Get get = new Get(Bytes.toBytes("row1"));
        get.setMaxVersions();
        get.addFamily(Bytes.toBytes("cf"));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"));
        get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"));
        Result result = table.get(get);

        for (Cell cell : result.rawCells()) {
            System.out.println(String.format("row=%s,family=%s,qualifier=%s,timestamp=%s,value=%s",
                    Bytes.toString(CellUtil.cloneRow(cell)), Bytes.toString(CellUtil.cloneFamily(cell)), Bytes.toString(CellUtil.cloneQualifier(cell)), cell.getTimestamp(), Bytes.toString(CellUtil.cloneValue(cell))));
        }
    }

    private static void put(Table table) throws IOException {
        Put put = new Put(Bytes.toBytes("row6"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"), Bytes.toBytes("666"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("name"), Bytes.toBytes("dali666"));
        put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"), Bytes.toBytes(25));
        table.put(put);
    }

    private static void delete(Table table) throws IOException {
        Delete delete = new Delete(Bytes.toBytes("row6"));
        //delete.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("age"));
        delete.addFamily(Bytes.toBytes("cf"));
        table.delete(delete);
    }

    private static void scan(Table table) throws IOException {
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes("row1"));
        scan.setStopRow(Bytes.toBytes("row3"));
        scan.addFamily(Bytes.toBytes("cf"));
        scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("id"));

        scan.setCacheBlocks(true);//将结果放到blockCache中
        scan.setCaching(100);//每一次获取多少条数据
        ResultScanner rs = table.getScanner(scan);

        for (Result result : rs) {
            System.out.println(Bytes.toString(result.getRow()));
            System.out.println(Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("id"))));
            System.out.println("===");
        }
    }
}