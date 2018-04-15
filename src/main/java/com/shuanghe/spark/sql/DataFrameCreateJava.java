package com.shuanghe.spark.sql;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-15
 * Time: 下午8:22
 * To change this template use File | Settings | File Templates.
 * Description:创建 DataFrame
 */
public class DataFrameCreateJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("DataFrameCreate")
                .master("local")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        //SQLContext sqlContext=new SQLContext(sc);
        SQLContext sqlContext = new SQLContext(spark);

        Dataset<Row> df = spark.read()
                .option("timestampFormat", "yyyy/MM/dd HH:mm:ss ZZ")
                .json("file:///home/yushuanghe/test/data/students.json");

        df.show();

        spark.close();
    }
}