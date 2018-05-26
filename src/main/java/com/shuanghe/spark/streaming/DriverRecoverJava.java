package com.shuanghe.spark.streaming;

import kafka.serializer.StringDecoder;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.util.LongAccumulator;
import scala.Tuple2;

import java.util.*;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-11
 * Time: 下午11:10
 * To change this template use File | Settings | File Templates.
 * Description:从checkpoint中恢复driver
 * <p>
 * Accumulator 和 Broadcast 无法从Spark Streaming中的checkpoint恢复。如果启用了checkpoint，用蓄电池或广播的变量，以及，你必须创建一个蓄电池和广播变量lazy地实例化单实例，以便它们可以在故障的驱动程序重新启动后重新实例化。这在以下示例中显示。
 */
public class DriverRecoverJava {
    private static JavaStreamingContext createContext(String checkpointDirectory) {
        SparkSession spark = SparkSession.builder()
                .appName("DriverRecoverJava")
                .master("local[2]")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaStreamingContext ssc = new JavaStreamingContext(sc, Durations.seconds(5));

        // 第一点，如果要使用 updateStateByKey 算子，就必须设置一个checkpoint目录，开启checkpoint机制
        // 这样的话才能把每个key对应的state除了在内存中有，那么是不是也要checkpoint一份
        // 因为你要长期保存一份key的state的话，那么spark streaming是要求必须用checkpoint的，以便于在内存数据丢失的时候，可以从checkpoint中恢复数据
        ssc.checkpoint(checkpointDirectory);

        Map<String, String> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", "shuanghe.com:9092");

        Set<String> topics = new HashSet<>();
        topics.add("test");

        JavaPairDStream<String, String> messages = KafkaUtils.createDirectStream(ssc, String.class, String.class, StringDecoder.class, StringDecoder.class, kafkaParams, topics);

        JavaDStream<String> lines = messages.map((Tuple2<String, String> tuple) -> tuple._2);

        JavaPairDStream<String, Integer> wordcounts = lines
                .flatMap((FlatMapFunction<String, String>) line -> Arrays.asList(line.split(" ")).iterator())
                .mapToPair((PairFunction<String, String, Integer>) word -> new Tuple2<>(word, 1));
        //.reduceByKey((Function2<Integer, Integer, Integer>) (v1, v2) -> v1 + v2);

        JavaPairDStream<String, Integer> result = wordcounts.updateStateByKey(
                //Optional相当于scala中的样例类 Option
                (Function2<List<Integer>, org.apache.spark.api.java.Optional<Integer>, org.apache.spark.api.java.Optional<Integer>>) (values, state) -> {
                    // 这里两个参数
                    // 实际上，对于每个单词，每次batch计算的时候，都会调用这个函数
                    // 第一个参数，values，相当于是这个batch中，这个key的新的值，可能有多个吧
                    // 比如说一个hello，可能有2个1，(hello, 1) (hello, 1)，那么传入的是(1,1)
                    // 第二个参数，就是指的是这个key之前的状态，state，其中泛型的类型是你自己指定的

                    // 首先定义一个全局的单词计数
                    Integer newValue = 0;

                    // 其次，判断，state是否存在，如果不存在，说明是一个key第一次出现
                    // 如果存在，说明这个key之前已经统计过全局的次数了
                    if (state.isPresent()) {
                        newValue = state.get();
                    }

                    // 接着，将本次新出现的值，都累加到newValue上去，就是一个key目前的全局的统计
                    // 次数
                    for (Integer value : values) {
                        newValue += value;
                    }

                    return Optional.of(newValue);
                });

        result.checkpoint(Durations.seconds(10));
        result.print();

        result.foreachRDD((VoidFunction<JavaPairRDD<String, Integer>>) (rdd) -> {
            LongAccumulator accumulator = JavaDroppedWordsCounter.getInstance(JavaSparkContext.fromSparkContext(rdd.context()));
            rdd.foreach((VoidFunction<Tuple2<String, Integer>>) (tuple) -> {
                accumulator.add(tuple._2);
                System.out.println(tuple);
            });
            System.out.println("accumulator: " + accumulator.value());
        });

        return ssc;
    }

    public static void main(String[] args) {
        String checkpointDirectory = "hdfs://shuanghe.com:8020/user/yushuanghe/spark/streaming/driverRecover";
        Function0<JavaStreamingContext> createContextFunc =
                () -> createContext(checkpointDirectory);

        JavaStreamingContext ssc =
                JavaStreamingContext.getOrCreate(checkpointDirectory, createContextFunc);

        ssc.start();
        try {
            ssc.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}

class JavaWordBlacklist {

    private static volatile Broadcast<List<String>> instance = null;

    public static Broadcast<List<String>> getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaWordBlacklist.class) {
                if (instance == null) {
                    List<String> wordBlacklist = Arrays.asList("a", "b", "c");
                    instance = jsc.broadcast(wordBlacklist);
                }
            }
        }
        return instance;
    }
}

class JavaDroppedWordsCounter {

    private static volatile LongAccumulator instance = null;

    public static LongAccumulator getInstance(JavaSparkContext jsc) {
        if (instance == null) {
            synchronized (JavaDroppedWordsCounter.class) {
                if (instance == null) {
                    instance = jsc.sc().longAccumulator("WordsInBlacklistCounter");
                }
            }
        }
        return instance;
    }
}