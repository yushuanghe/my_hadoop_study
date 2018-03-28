package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-27
 * Time: 下午4:28
 * To change this template use File | Settings | File Templates.
 * Description:transformation操作
 */
@SuppressWarnings(value = {"unused", "unchecked"})
public class TransformationOperationJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("TransformationOperation")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5);

        JavaRDD<Integer> rdd = sc.parallelize(list, 2);

        //map(rdd);
        //filter(rdd);
        //flatMap(rdd);
        //groupByKey(sc);
        //reduceByKey(sc);
        //sortByKey(sc);
        //join(sc);
        cogroup(sc);

        sc.close();
    }

    private static void map(JavaRDD<Integer> rdd) {
        JavaRDD<Integer> newRdd = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) {
                return v1 * 2;
            }
        });

        newRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer i) {
                System.out.println(i);
            }
        });
    }

    private static void filter(JavaRDD<Integer> rdd) {
        JavaRDD<Integer> newRdd = rdd.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) {
                return v1 % 2 == 1;
            }
        });

        newRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) {
                System.out.println(integer);
            }
        });
    }

    private static void flatMap(JavaRDD<Integer> rdd) {
        JavaRDD<String> newRdd = rdd.flatMap(new FlatMapFunction<Integer, String>() {
            @Override
            public Iterator<String> call(Integer integer) throws Exception {
                List<String> list = new ArrayList<>();
                for (int i = 0; i < integer; i++) {
                    list.add(integer + ":" + i);
                }
                return list.iterator();
            }
        });

        newRdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) {
                System.out.println(s);
            }
        });
    }

    private static void groupByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<>("class1", 80), new Tuple2<>("class2", 75), new Tuple2<>("class1", 90), new Tuple2<>("class2", 88));

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scores, 2);

        JavaPairRDD<String, Iterable<Integer>> groupRdd = rdd.groupByKey();

        groupRdd.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                //for (Integer i:tuple._2){
                //    System.out.println(tuple._1+":"+i);
                //}

                System.out.println("class: " + tuple._1);

                for (Integer a_2 : tuple._2) {
                    System.out.println(a_2);
                }
                System.out.println("==============================");
            }
        });
    }

    private static void reduceByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<>("class1", 80), new Tuple2<>("class2", 75), new Tuple2<>("class1", 90), new Tuple2<>("class2", 88));

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scores, 2);

        JavaPairRDD<String, Integer> reduceRdd = rdd.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduceRdd.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple);
            }
        });
    }

    private static void sortByKey(JavaSparkContext sc) {
        List<Tuple2<String, Integer>> scores = Arrays.asList(new Tuple2<>("class1", 80), new Tuple2<>("class2", 75), new Tuple2<>("class1", 90), new Tuple2<>("class2", 88));

        JavaPairRDD<String, Integer> rdd = sc.parallelizePairs(scores, 2);

        JavaPairRDD<Integer, String> rdd1 = rdd.mapToPair(new PairFunction<Tuple2<String, Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<>(tuple._2, tuple._1);
            }
        });

        JavaPairRDD<Integer, String> sortRdd = rdd1.sortByKey(false);

        JavaPairRDD<String, Integer> rdd3 = sortRdd.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                return new Tuple2<>(tuple._2, tuple._1);
            }
        });

        rdd3.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple);
            }
        });
    }

    private static void join(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60)
        );

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "haha")
        );

        JavaPairRDD<Integer, String> studentRdd = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRdd = sc.parallelizePairs(scoreList);

        //def join[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (V, W)]
        //   JavaPairRDD<Integer, Tuple2<String, Integer>> joinRdd=
        studentRdd.join(scoreRdd)
                .foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
                    @Override
                    public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple) throws Exception {
                        System.out.println(tuple._1 + ":" + tuple._2._1 + "," + tuple._2._2);
                    }
                });
    }

    private static void cogroup(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<>(1, 50),
                new Tuple2<>(1, 99)
        );

        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "haha")
        );

        JavaPairRDD<Integer, String> studentRdd = sc.parallelizePairs(studentList);
        JavaPairRDD<Integer, Integer> scoreRdd = sc.parallelizePairs(scoreList);

        //def cogroup[W](other: JavaPairRDD[K, W]): JavaPairRDD[K, (JIterable[V], JIterable[W])]
        //JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> joinRdd=
        studentRdd.cogroup(scoreRdd)
                .foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
                    @Override
                    public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> tuple) throws Exception {
                        System.out.println(tuple._1 + ":" + tuple._2._1 + "," + tuple._2._2);
                    }
                });
    }
}