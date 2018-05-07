package com.shuanghe.spark.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-6
 * Time: 下午9:32
 * To change this template use File | Settings | File Templates.
 * Description:
 */
public class KafkaReceiverWordCount {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("KafkaReceiverWordCount")
                .master("local[2]")
                .getOrCreate();

        JavaStreamingContext ssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(5));

        ssc.checkpoint("spark/streaming/checkpoint");

        String zkQuorum = "shuanghe.com:2181";
        String group = "defaultConsumerGroup";
        Map<String, Integer> topicThreadMap = new HashMap<>();
        topicThreadMap.put("test", 1);

        //使用KafkaUtils.createStream()方法，创建针对Kafka的输入数据流
        JavaDStream<String> lines = KafkaUtils.createStream(ssc, zkQuorum, group, topicThreadMap)
                .map((Tuple2<String, String> input) -> input._2);

        JavaDStream<String> words = lines.flatMap((String line) -> Arrays.asList(line.split(" ")).iterator());

        JavaPairDStream<String, Integer> pairs = words.mapToPair((String word) -> new Tuple2<>(word, 1));

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