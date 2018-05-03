package com.shuanghe.spark.streaming;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-5-4
 * Time: 上午12:16
 * To change this template use File | Settings | File Templates.
 * Description:实时 wordcount 程序
 */
public class WordCountJava {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .appName("WordCountJava")
                .master("local[2]")
                .getOrCreate();

        // 还必须接收一个batch interval参数，就是说，每收集多长时间的数据，划分为一个batch，进行处理
        // 这里设置一秒
        JavaStreamingContext streamingContext = new JavaStreamingContext(JavaSparkContext.fromSparkContext(spark.sparkContext()), Durations.seconds(5));

        //首先，创建输入DStream，代表了一个从数据源来的持续不断的实时数据源
        //调用 JavaStreamingContext 的 socketTextStream 方法，可以创建一个数据源为socket网络端口的数据流
        //JavaReceiverInputDStream 代表了一个输入的 DStream
        JavaReceiverInputDStream<String> lines = streamingContext.socketTextStream("localhost", 9999);

        // 到这里为止，可以理解为 JavaReceiverInputDStream 中的，每隔5秒，会有一个RDD，其中封装了
        // 这5秒发送过来的数据
        // RDD的元素类型为String，即一行一行的文本
        // 所以，这里 JavaReceiverInputStream 的泛型类型 <String> ，其实就代表了它底层的RDD的泛型类型

        // 开始对接收到的数据，执行计算，使用Spark Core提供的算子，直接应用在DStream中即可
        // 在底层，实际上是会对DStream中的一个一个的RDD，执行我们应用在DStream上的算子
        // 产生的新RDD，会作为新DStream中的RDD
        JavaDStream<String> words = lines.flatMap((String line) ->
                Arrays.asList(line.split(" ")).iterator());

        // 这个时候，每5秒的数据，一行一行的文本，就会被拆分为多个单词，words DStream中的RDD的元素类型
        // 即为一个一个的单词

        JavaPairDStream<String, Integer> pairs = words.mapToPair((String word) -> new Tuple2<>(word, 1));

        // 这里，正好说明一下，其实大家可以看到，用Spark Streaming开发程序，和Spark Core很相像
        // 唯一不同的是Spark Core中的JavaRDD、JavaPairRDD，都变成了JavaDStream、JavaPairDStream

        JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((Integer v1, Integer v2) -> v1 + v2);

        // 到此为止，就实现了实时的wordcount程序了
        // 总结一下思路，加深一下印象
        // 每5秒中发送到指定socket端口上的数据，都会被lines DStream接收到
        // 然后lines DStream会把每5秒的数据，也就是一行一行的文本，诸如hello world，封装为一个RDD
        // 然后呢，就会对每秒中对应的RDD，执行后续的一系列的算子操作
        // 比如，对lins RDD执行了flatMap之后，得到一个words RDD，作为words DStream中的一个RDD
        // 以此类推，直到生成最后一个，wordCounts RDD，作为wordCounts DStream中的一个RDD
        // 此时，就得到了，每5秒钟发送过来的数据的单词统计
        // 但是，一定要注意，Spark Streaming的计算模型，就决定了，我们必须自己来进行中间缓存的控制
        // 比如写入redis等缓存
        // 它的计算模型跟Storm是完全不同的，storm是自己编写的一个一个的程序，运行在节点上，相当于一个
        // 一个的对象，可以自己在对象中控制缓存
        // 但是Spark本身是函数式编程的计算模型，所以，比如在words或pairs DStream中，没法在实例变量中
        // 进行缓存
        // 此时就只能将最后计算出的wordCounts中的一个一个的RDD，写入外部的缓存，或者持久化DB

        // 最后，每次计算完，都打印一下这一秒钟的单词计数情况
        wordCounts.print();

        // 首先对JavaSteamingContext进行一下后续处理
        // 必须调用JavaStreamingContext的start()方法，整个Spark Streaming Application才会启动执行
        // 否则是不会执行的
        streamingContext.start();
        try {
            streamingContext.awaitTermination();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        streamingContext.close();
    }
}