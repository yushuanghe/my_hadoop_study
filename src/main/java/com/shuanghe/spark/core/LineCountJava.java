package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-27
 * Time: 下午4:07
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class LineCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("LineCountJava")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> lines = sc.textFile("mapreduce/input", 1);

        //lines.mapToPair(x->new Tuple2<>(x,1));
        JavaPairRDD<String, Integer> ones = lines.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        counts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1 + ":" + tuple._2);
            }
        });

        sc.close();
    }
}