package com.shuanghe.spark.test;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

/**
 * Description:下列代码有几个task
 * <p>
 * Date: 2018/06/22
 * Time: 19:47
 *
 * @author yushuanghe
 */
public class TaskNum {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TaskNum")
                .master("local")
                .getOrCreate();

        JavaSparkContext.fromSparkContext(spark.sparkContext())
                .textFile("file:///Users/yushuanghe/Desktop/file.txt")
                .mapToPair(line -> new Tuple2<>(line, 1))
                .reduceByKey((Integer v1, Integer v2) -> v1 + v2)
                .cache()
                .map(tuple -> tuple._1)
                .collect();
    }
}