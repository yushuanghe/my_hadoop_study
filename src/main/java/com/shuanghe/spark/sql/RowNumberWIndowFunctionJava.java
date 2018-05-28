package com.shuanghe.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-25
 * Time: 下午11:18
 * To change this template use File | Settings | File Templates.
 * Description:row_number()窗口函数实战
 */
public class RowNumberWIndowFunctionJava {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        //SparkConf conf = new SparkConf()
        //        .setAppName("RowNumberWindowFunction");
        //JavaSparkContext sc = new JavaSparkContext(conf);
        //HiveContext hiveContext = new HiveContext(sc.sc());

        SparkSession spark = SparkSession.builder()
                .appName("SaveModeTest")
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()//2.0写法
                .getOrCreate();

        // 创建销售额表，sales表
        spark.sql("use test");
        spark.sql("DROP TABLE IF EXISTS sales");
        spark.sql("CREATE TABLE IF NOT EXISTS sales ("
                + "product STRING,"
                + "category STRING,"
                + "revenue BIGINT)");
        spark.sql("LOAD DATA "
                + "LOCAL INPATH '/home/yushuanghe/test/data/sales.txt' "
                + "INTO TABLE sales");

        // 开始编写我们的统计逻辑，使用row_number()开窗函数
        // 先说明一下，row_number()开窗函数的作用
        // 其实，就是给每个分组的数据，按照其排序顺序，打上一个分组内的行号
        // 比如说，有一个分组date=20151001，里面有3条数据，1122，1121，1124,
        // 那么对这个分组的每一行使用row_number()开窗函数以后，三行，依次会获得一个组内的行号
        // 行号从1开始递增，比如1122 1，1121 2，1124 3

        Dataset<Row> top3SalesDF = spark.sql(""
                + "SELECT product,category,revenue "
                + "FROM ("
                + "SELECT "
                + "product,"
                + "category,"
                + "revenue,"
                // row_number()开窗函数的语法说明
                // 首先可以，在SELECT查询时，使用row_number()函数
                // 其次，row_number()函数后面先跟上OVER关键字
                // 然后括号中，是PARTITION BY，也就是说根据哪个字段进行分组
                // 其次是可以用ORDER BY进行组内排序
                // 然后row_number()就可以给每个组内的行，一个组内行号
                + "row_number() OVER (PARTITION BY category ORDER BY revenue DESC) rank "
                + "FROM sales "
                + ") tmp_sales "
                + "WHERE rank<=3");

        // 将每组排名前3的数据，保存到一个表中
        spark.sql("DROP TABLE IF EXISTS top3_sales");
        top3SalesDF.write().saveAsTable("top3_sales");

        spark.close();
    }
}