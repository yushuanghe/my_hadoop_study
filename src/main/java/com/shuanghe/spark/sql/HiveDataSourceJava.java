package com.shuanghe.spark.sql;

import org.apache.spark.sql.SparkSession;

import java.io.File;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-4-18
 * Time: 下午11:22
 * To change this template use File | Settings | File Templates.
 * Description:hive数据源
 */
public class HiveDataSourceJava {
    public static void main(String[] args) {
        String warehouseLocation = new File("spark-warehouse").getAbsolutePath();

        SparkSession spark = SparkSession.builder()
                .appName("HiveDataSource")
                .master("local")
                .config("spark.sql.warehouse.dir", warehouseLocation)
                .enableHiveSupport()//2.0写法
                .getOrCreate();

        //2.0之前写法
        //HiveContext hiveContext = new HiveContext(JavaSparkContext.fromSparkContext(spark.sparkContext()));

        //Exception in thread "main" java.lang.NoSuchFieldError: METASTORE_CLIENT_SOCKET_LIFETIME
        //可能是hive版本低导致

        spark.sql("use test");
        spark.sql("drop table if exists student_info");
        spark.sql("create table if not exists student_info (name string,age int)");
        spark.sql("load data local inpath \"/home/yushuanghe/test/data/student_infos.txt\" into table student_info");

        spark.sql("drop table if exists student_score");
        spark.sql("create table if not exists student_score (name string,score int)");
        spark.sql("load data local inpath \"/home/yushuanghe/test/data/student_scores.txt\" into table student_score");

        spark.close();
    }
}