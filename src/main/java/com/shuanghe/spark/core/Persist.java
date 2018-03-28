package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-28
 * Time: 上午11:46
 * To change this template use File | Settings | File Templates.
 * Description:持久化
 */
public class Persist {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("Persist")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("file:///home/yushuanghe/test/data/spark.txt")
                // cache()或者persist()的使用，是有规则的
                // 必须在transformation或者textFile等创建了一个RDD之后，直接连续调用cache()或persist()才可以
                // 如果你先创建一个RDD，然后单独另起一行执行cache()或persist()方法，是没有用的
                .cache();

        //cache无效
        //rdd.cache();

        long startTime = System.currentTimeMillis();
        long a = rdd.count();
        System.out.println(a);
        long endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);

        startTime = System.currentTimeMillis();
        a = rdd.count();
        System.out.println(a);
        endTime = System.currentTimeMillis();
        System.out.println(endTime - startTime);

        sc.close();
    }
}