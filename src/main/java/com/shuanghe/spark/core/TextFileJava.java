package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-26
 * Time: 下午11:40
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class TextFileJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("TextFile")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("file:///home/yushuanghe/test/data/spark.txt");

        JavaRDD<Integer> lengthRdd = rdd.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        });

        int sum = lengthRdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(sum);

        sc.close();
    }
}