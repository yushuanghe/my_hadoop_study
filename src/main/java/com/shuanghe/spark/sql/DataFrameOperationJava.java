package com.shuanghe.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-15
 * Time: 下午9:58
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class DataFrameOperationJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameOperationJava")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/students.json");

        df.show();

        df.printSchema();

        df.select("name").show();

        df.select(df.col("name"), df.col("age").plus(100)).show();

        df.filter(df.col("age").gt(18)).show();

        df.groupBy(df.col("age")).count().show();

        spark.close();
    }
}