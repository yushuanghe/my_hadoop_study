package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-29
 * Time: 上午12:49
 * To change this template use File | Settings | File Templates.
 * Description:取最大的前3个数字
 */
public class Top3Java {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Top3")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("file:///home/yushuanghe/test/data/top.txt", 2);

        JavaRDD<String> resultRdd = rdd.mapToPair(x -> new Tuple2<>(Integer.parseInt(x), x))
                .sortByKey(false)
                .map(x -> x._2);

        resultRdd.foreach(x -> System.out.println(x));

        sc.close();
    }
}