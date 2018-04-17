package com.shuanghe.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-17
 * Time: 上午12:55
 * To change this template use File | Settings | File Templates.
 * Description:parquet 自动推断分区
 */
public class ParquetPartitionDiscoveryJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("ParquetPartitionDiscovery")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                //开启自动类型推断
                .option("spark.sql.sources.partitionColumnTypeInference.enabled", "true")
                .parquet("spark/user");

        df.printSchema();
        df.show();

        spark.close();
    }
}