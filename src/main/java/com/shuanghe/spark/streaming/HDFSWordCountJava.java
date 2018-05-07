package com.shuanghe.spark.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-6
 * Time: 下午4:45
 * To change this template use File | Settings | File Templates.
 * Description:基于HDFS文件的实时wordcount程序
 */
public class HDFSWordCountJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("HDFSWordCountJava")
                .master("local[2]")
                .getOrCreate();

        JavaStreamingContext ssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(5));

        JavaDStream<String> lines = ssc.textFileStream("hdfs://shuanghe.com:8020/user/yushuanghe/spark/wordcount_dir");

        JavaDStream<String> words = lines.flatMap((String line) ->
                Arrays.asList(line.split(" ")).iterator()
        );

        JavaPairDStream<String, Integer> pairs = words.mapToPair((String word) ->
                new Tuple2<>(word, 1));

        JavaPairDStream<String, Integer> wordcounts = pairs.reduceByKey((Integer v1, Integer v2) -> v1 + v2);

        wordcounts.print();

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}