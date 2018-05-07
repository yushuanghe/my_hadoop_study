package com.shuanghe.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-6
 * Time: 下午10:25
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class KafkaDirectWordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaDirectWordCount")
                .master("local[2]")
                .getOrCreate();

        JavaStreamingContext ssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(5));

        ssc.checkpoint("spark/streaming/checkpoint");

        //创建kafka参数map
        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "shuanghe.com:9092");

        //创建topic set
        //可以并行读取多个topic
        Set<String> topics = new HashSet<>();
        topics.add("test");

        //创建输入DStream
        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        JavaDStream<String> lines = messages.map((Tuple2<String, String> tuple) -> tuple._2);

        JavaPairDStream wordcounts = lines
                .flatMap((String line) -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair((String word) -> new Tuple2<>(word, 1))
                .reduceByKey((Integer v1, Integer v2) -> v1 + v2);

        wordcounts.print();

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}