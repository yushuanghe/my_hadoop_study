package com.shuanghe.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-16
 * Time: 下午11:05
 * To change this template use File | Settings | File Templates.
 * Description:通用的load和save操作
 */
public class GenericLoadSaveJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("GenericLoadSave")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read().load("file:///home/yushuanghe/test/data/users.parquet");

        df.printSchema();

        df.show();

        df.select("name", "favorite_numbers").write()
                .save("spark/namesAndColors");

        spark.close();
    }
}