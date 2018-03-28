package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-28
 * Time: 下午11:57
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class SortWordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("SortWordCountJava")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> rdd = sc.textFile("file:///home/yushuanghe/test/data/spark.txt");

        JavaPairRDD<String, Integer> pairRdd = rdd.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" ")).iterator();
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> wordCountRdd = pairRdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        JavaPairRDD<Integer, String> rdd1 = wordCountRdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<>(tuple._2, tuple._1);
            }
        });

        JavaPairRDD<String, Integer> rdd2 = rdd1.sortByKey(false)
                .mapToPair(x -> new Tuple2<>(x._2, x._1));

        List<Tuple2<String, Integer>> list = rdd2.take(5);

        for (Tuple2<String, Integer> tuple : list) {
            System.out.println(tuple);
        }

        sc.close();
    }
}