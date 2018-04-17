package com.shuanghe.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-17
 * Time: 上午12:16
 * To change this template use File | Settings | File Templates.
 * Description:parquet 数据源之使用编程方式加载数据
 */
public class ParquetLoadDataJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ParquetLoadData")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().parquet("file:///home/yushuanghe/test/data/users.parquet");

        df.createOrReplaceTempView("user");

        df.show();

        Dataset<Row> nameDF = spark.sql("select name from user");

        JavaRDD<String> rdd = nameDF.javaRDD()
                .map((Row row) ->
                        "name:" + row.getAs("name")
                );

        rdd.foreach(x -> System.out.println(x));

        spark.close();
    }
}