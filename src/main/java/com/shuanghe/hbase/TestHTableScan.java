package com.shuanghe.hbase;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by yushuanghe on 2017/02/16.
 */
public class TestHTableScan {
    public static void main(String[] args) throws IOException {
        Configuration conf = HBaseUtil.getHBaseConfiguration();
        HTable hTable = null;

        testUseHBaseConnectionPool(conf);
    }

    /**
     * 测试scan
     *
     * @param hTable
     * @throws IOException
     */
    static void testScan(HTableInterface hTable) throws IOException {
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("row1"));
        scan.setStopRow(Bytes.toBytes("row5"));

        /**
         * !AND
         * MUST_PASS_ALL,
         * !OR
         * MUST_PASS_ONE
         */
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        byte[][] prefixes = new byte[2][];
        prefixes[0] = Bytes.toBytes("id");
        prefixes[1] = Bytes.toBytes("name");
        MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(prefixes);
        filterList.addFilter(mcpf);

        scan.setFilter(filterList);

        ResultScanner rs = hTable.getScanner(scan);
        for (Result result : rs) {
            printResult(result);
        }
    }

    /**
     * 打印result
     *
     * @param result
     */
    static void printResult(Result result) {
        Cell[] cells = result.rawCells();

        for (Cell cell : cells) {
            System.out.println(Bytes.toString(cell.getRow()) + "," + Bytes.toString(cell.getFamily()) + "," +
                    Bytes.toString(cell.getQualifier()) + "," + Bytes.toString(cell.getValue()));
        }

        System.out.println("===============");

        NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> map = result.getMap();
        for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long, byte[]>>> entry : map.entrySet()) {
            String family = Bytes.toString(entry.getKey());
            for (Map.Entry<byte[], NavigableMap<Long, byte[]>> columnEntry : entry.getValue().entrySet()) {
                String column = Bytes.toString(columnEntry.getKey());
                String value = Bytes.toString(columnEntry.getValue().firstEntry().getValue());
                System.out.println(Bytes.toString(result.getRow()) + "," + family + "," + column + "," + value);
            }
        }

        System.out.println("===============");
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

            testScan(hTable);
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
}
