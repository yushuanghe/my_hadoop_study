package com.shuanghe.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
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
 * Date: 18-5-8
 * Time: 上午12:04
 * To change this template use File | Settings | File Templates.
 * Description:基于 transform 的实时黑名单过滤
 */
public class TransformBlacklistJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("TransformBlacklistJava")
                .master("local[2]")
                .getOrCreate();

        JavaStreamingContext ssc = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(5));
        ssc.checkpoint("spark/streaming/checkpoint");

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "shuanghe.com:9092");

        Set<String> topics = new HashSet<>();
        topics.add("test");

        // 用户对我们的网站上的广告可以进行点击
        // 点击之后，是不是要进行实时计费，点一下，算一次钱
        // 但是，对于那些帮助某些无良商家刷广告的人，那么我们有一个黑名单
        // 只要是黑名单中的用户点击的广告，我们就给过滤掉

        // 先做一份模拟的黑名单RDD
        List<Tuple2<String, Boolean>> blacklistData = new ArrayList<>();
        blacklistData.add(new Tuple2<>("tom", true));
        JavaPairRDD<String, Boolean> blacklistRDD = ssc.sparkContext()
                .parallelizePairs(blacklistData);

        // 这里的日志格式，就简化一下，就是date username的方式
        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        JavaDStream<String> logs = messages.map((Function<Tuple2<String, String>, String>) tuple -> tuple._2);

        // 所以，要先对输入的数据，进行一下转换操作，变成，(username, date username)
        // 以便于，后面对每个batch RDD，与定义好的黑名单RDD进行join操作
        JavaPairDStream<String, String> userAdsClickLogDStream = logs.mapToPair((PairFunction<String, String, String>) log ->
                new Tuple2<>(log.split(" ")[1], log));

        // 然后，就可以执行transform操作了，将每个batch的RDD，与黑名单RDD进行join、filter、map等操作
        // 实时进行黑名单过滤
        JavaDStream<String> validAdsClickLogDStream = userAdsClickLogDStream.transform((Function<JavaPairRDD<String, String>, JavaRDD<String>>) userAdsClickLogRDD -> {
            //左外连接
            //因为，并不是每个用户都在黑名单中的
            //所以，直接使用join，那么不在黑名单的数据，会无法join到，被丢弃了
            //所以，使用 leftOuterJoin
            JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userAdsClickLogRDD.leftOuterJoin(blacklistRDD);

            //链接之后，执行filter算子
            JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD.filter((Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>) tuple -> {
                if (tuple._2._2.isPresent()) {
                    return !tuple._2._2.get();
                } else {
                    return true;
                }
            });

            // 此时，filteredRDD中，就只剩下没有被黑名单过滤的用户点击了
            // 进行map操作，转换成我们想要的格式
            JavaRDD<String> validAdsClickLogRDD = filteredRDD.map((Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, String>) tuple -> tuple._2._1
            );

            return validAdsClickLogRDD;
        });

        // 打印有效的广告点击日志
        // 其实在真实企业场景中，这里后面就可以走写入kafka、ActiveMQ等这种中间件消息队列
        // 然后再开发一个专门的后台服务，作为广告计费服务，执行实时的广告计费，这里就是只拿到了有效的广告点击
        validAdsClickLogDStream.print();

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}