package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-26
 * Time: 下午4:27
 * To change this template use File | Settings | File Templates.
 * Description:java7语法spark core
 */
public class WordCount {
    public static void main(String[] args) {
        //SparkSession spark = SparkSession
        //        .builder()
        //        .appName("JavaWordCount")
        //        .master("local")
        //        .getOrCreate();
        //JavaRDD<String> lines = spark.read().textFile(args[0]).javaRDD();

        SparkConf conf = new SparkConf()
                .setAppName("WordCountLocal")
                //.setMaster("local")
                ;

        // 在Spark中，SparkContext是Spark所有功能的一个入口，你无论是用java、scala，甚至是python编写
        // 都必须要有一个SparkContext，它的主要作用，包括初始化Spark应用程序所需的一些核心组件，包括
        // 调度器（DAGSchedule、TaskScheduler），还会去到Spark Master节点上进行注册，等等
        // 一句话，SparkContext，是Spark应用中，可以说是最最重要的一个对象
        // 但是呢，在Spark中，编写不同类型的Spark应用程序，使用的SparkContext是不同的，如果使用scala，
        // 使用的就是原生的SparkContext对象
        // 但是如果使用Java，那么就是JavaSparkContext对象
        // 如果是开发Spark SQL程序，那么就是SQLContext、HiveContext
        // 如果是开发Spark Streaming程序，那么就是它独有的SparkContext
        // 以此类推
        JavaSparkContext sc = new JavaSparkContext(conf);

        //JavaRDD<String> lines=sc.textFile("file:///home/yushuanghe/test/data/spark.txt");
        JavaRDD<String> lines = sc.textFile("mapreduce/input");

        //JavaPairRDD<String,Integer>counts= lines.flatMap(x->Arrays.asList(x.split(" ")).iterator())
        // .mapToPair(x->new Tuple2<>(x,1))
        // .reduceByKey((a,b)->a+b);
        //
        //List<Tuple2<String,Integer>> output= counts.collect();
        //
        //for(Tuple2<String,Integer> tuple:output){
        //    System.out.println(tuple._1() + ": " + tuple._2());
        //}

        //FlatMapFunction，有两个泛型参数，分别代表了输入和输出类型
        JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {

                return Arrays.asList(s.split(" ")).iterator();
            }
        });

        // mapToPair，其实就是将每个元素，映射为一个(v1,v2)这样的Tuple2类型的元素
        // 如果大家还记得scala里面讲的tuple，那么没错，这里的tuple2就是scala类型，包含了两个值
        // mapToPair这个算子，要求的是与PairFunction配合使用，第一个泛型参数代表了输入类型
        // 第二个和第三个泛型参数，代表的输出的Tuple2的第一个值和第二个值的类型
        // JavaPairRDD的两个泛型参数，分别代表了tuple元素的第一个值和第二个值的类型
        JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<>(s, 1);
            }
        });

        //reduceByKey会有本地combiner优化
        JavaPairRDD<String, Integer> wordCounts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        wordCounts.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1 + " appeared " + tuple._2 + " times.");
            }
        });

        sc.stop();
    }
}