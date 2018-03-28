package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-26
 * Time: 下午11:23
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class ParallelizeCollectionJava {
    public static void main(String[] args) {
        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        //SparkSession spark = SparkSession
        //        .builder()
        //        .appName("JavaWordCount")
        //        .master("local")
        //        .getOrCreate();
        SparkConf conf = new SparkConf()
                .setAppName("ParallelizeCollection")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> numberRDD = sc.parallelize(list);

        int sum = numberRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(sum);

        sc.close();
    }
}