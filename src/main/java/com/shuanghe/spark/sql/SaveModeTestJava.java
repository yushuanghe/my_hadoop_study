package com.shuanghe.spark.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-16
 * Time: 下午11:50
 * To change this template use File | Settings | File Templates.
 * Description:save mode
 */
public class SaveModeTestJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("SaveModeTest")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/people.json");

        df.write()
                .mode(SaveMode.Ignore)
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .csv("spark/csvOutput");

        spark.close();
    }
}