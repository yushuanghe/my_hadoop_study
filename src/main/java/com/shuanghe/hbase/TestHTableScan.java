package com.shuanghe.hbase;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.MultipleColumnPrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created by yushuanghe on 2017/02/16.
 */
public class TestHTableScan {
    public static void main(String[] args) {
        Connection connection = null;
        Table table = null;
        TableName tableName = TableName.valueOf("test");

        try {
            connection = HBaseUtil.getCon();
            table = connection.getTable(tableName);

            testScan(table);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(connection, null, table);
        }
    }

    /**
     * 测试scan
     *
     * @param table
     * @throws IOException
     */
    private static void testScan(Table table) throws IOException {
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("row1"));
        scan.setStopRow(Bytes.toBytes("row3"));

        /*
         * MUST_PASS_ALL:AND
         * MUST_PASS_ONE:OR
         */
        FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);

        byte[][] prefixes = new byte[2][];
        prefixes[0] = Bytes.toBytes("id");
        prefixes[1] = Bytes.toBytes("name");
        MultipleColumnPrefixFilter mcpf = new MultipleColumnPrefixFilter(prefixes);
        filterList.addFilter(mcpf);

        scan.setFilter(filterList);

        ResultScanner rs = table.getScanner(scan);
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
            System.out.println(Bytes.toString(CellUtil.cloneRow(cell)) + "," + Bytes.toString(CellUtil.cloneFamily(cell)) + "," +
                    Bytes.toString(CellUtil.cloneQualifier(cell)) + "," +
                    cell.getTimestamp() + "," + Bytes.toString(CellUtil.cloneValue(cell)));
        }

        System.out.println("===============");

        /*
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
        */
    }
}
