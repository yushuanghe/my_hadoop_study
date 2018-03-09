package com.shuanghe.hbase.api;

import com.shuanghe.hbase.util.HBaseUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-9
 * Time: 上午1:44
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class ScanTest {
    private static String name = "test";
    private static String family = "cf";

    public static void main(String[] args) {
        Connection connection = null;
        Table table = null;
        TableName tableName = TableName.valueOf(name);

        try {
            connection = HBaseUtil.getCon();

            table = connection.getTable(tableName);

            testScan(table);
            System.out.println("分割线=========");
            testScan1(table);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            HBaseUtil.close(connection, null, table);
        }
    }

    private static void testScan(Table table) throws IOException {
        //Scan类常用方法说明
        //指定需要的family或column ，如果没有调用任何addFamily或Column，会返回所有的columns；
        // scan.addFamily();
        // scan.addColumn();
        // scan.setMaxVersions(); //指定最大的版本个数。如果不带任何参数调用setMaxVersions，表示取所有的版本。如果不掉用setMaxVersions，只会取到最新的版本.
        // scan.setTimeRange(); //指定最大的时间戳和最小的时间戳，只有在此范围内的cell才能被获取.
        // scan.setTimeStamp(); //指定时间戳
        // scan.setFilter(); //指定Filter来过滤掉不需要的信息
        // scan.setStartRow(); //指定开始的行。如果不调用，则从表头开始；
        // scan.setStopRow(); //指定结束的行（不含此行）；
        // scan.setBatch(); //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。

        //过滤器
        //1、FilterList代表一个过滤器列表
        //FilterList.Operator.MUST_PASS_ALL -->and
        //FilterList.Operator.MUST_PASS_ONE -->or
        //eg、FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ONE);
        //2、SingleColumnValueFilter
        //3、ColumnPrefixFilter用于指定列名前缀值相等
        //4、MultipleColumnPrefixFilter和ColumnPrefixFilter行为差不多，但可以指定多个前缀。
        //5、QualifierFilter是基于列名的过滤器。
        //6、RowFilter
        //7、RegexStringComparator是支持正则表达式的比较器。
        //8、SubstringComparator用于检测一个子串是否存在于值中，大小写不敏感。

        Scan scan = new Scan();
        scan.setMaxVersions();
        //指定最多返回的Cell数目。用于防止一行中有过多的数据，导致OutofMemory错误。
        //scan.setBatch(1000);

        //scan.setTimeStamp(NumberUtils.toLong("1370336286283"));
        //scan.setTimeRange(NumberUtils.toLong("1370336286283"), NumberUtils.toLong("1370336337163"));
        scan.setStartRow(Bytes.toBytes("row1"));
        scan.setStopRow(Bytes.toBytes("row3"));
        scan.addFamily(Bytes.toBytes(family));
        //scan.addColumn(Bytes.toBytes(family), Bytes.toBytes("id"));

        //查询列镞为cf，列id值为11的记录
        //方法一(单个查询)
        Filter filter = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes("id"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("11"));
        scan.setFilter(filter);

        //方法二(组合查询)
        //FilterList filterList = new FilterList();
        //Filter filter2 = new SingleColumnValueFilter(Bytes.toBytes(family), Bytes.toBytes("id"), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("11"));
        //filterList.addFilter(filter2);
        //scan.setFilter(filterList);

        ResultScanner rs = table.getScanner(scan);

        for (Result r : rs) {
            for (Cell cell : r.rawCells()) {
                System.out.println(String.format("row:%s, family:%s, qualifier:%s, qualifiervalue:%s, timestamp:%s.",
                        Bytes.toString(CellUtil.cloneRow(cell)),
                        Bytes.toString(CellUtil.cloneFamily(cell)),
                        Bytes.toString(CellUtil.cloneQualifier(cell)),
                        Bytes.toString(CellUtil.cloneValue(cell)),
                        cell.getTimestamp()));
            }
        }

        rs.close();
    }

    /**
     * result.getValue
     *
     * @param table
     * @throws IOException
     */
    private static void testScan1(Table table) throws IOException {
        //初始化Scan实例
        Scan scan = new Scan();

        scan.setStartRow(Bytes.toBytes("row1"));
        scan.setStopRow(Bytes.toBytes("row4"));
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes("id"));
        scan.addColumn(Bytes.toBytes(family), Bytes.toBytes("name"));

        //返回结果
        ResultScanner rs = table.getScanner(scan);

        //迭代并取出结果
        for (Result result : rs) {
            String id = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("id")));
            String name2 = Bytes.toString(result.getValue(Bytes.toBytes(family), Bytes.toBytes("name")));

            System.out.println(String.format("id=%s,name=%s", id, name2));
        }
    }
}