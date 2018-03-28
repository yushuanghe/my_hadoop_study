package com.shuanghe.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yushuanghe
 * Date: 18-3-28
 * Time: 上午10:01
 * To change this template use File | Settings | File Templates.
 * Description:action操作
 */
public class ActionOperationJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf()
                .setAppName("ActionOperationJava")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);

        JavaRDD<Integer> rdd = sc.parallelize(list, 2);

        //reduce(rdd);
        //collect(rdd);
        //count(rdd);
        //take(rdd);
        //saveAsTextFile(rdd);
        countByKey(sc);

        sc.close();
    }

    private static void reduce(JavaRDD<Integer> rdd) {
        int result = rdd.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 * v2;
            }
        });

        System.out.println(result);
    }

    private static void collect(JavaRDD<Integer> rdd) {
        JavaRDD<Integer> rdd1 = rdd.map(new Function<Integer, Integer>() {

            @Override
            public Integer call(Integer v1) throws Exception {
                return 2 * v1;
            }
        });

        //不建议使用，如果rdd数据比较大，全部返回driver，性能差
        //如果数据特别大，可能发生oom
        List<Integer> list = rdd1.collect();

        System.out.println(list);
    }

    private static void count(JavaRDD<Integer> rdd) {
        long a = rdd.count();
        System.out.println(a);
    }

    private static void take(JavaRDD<Integer> rdd) {
        List<Integer> a = rdd.take(3);
        System.out.println(a);
    }

    private static void saveAsTextFile(JavaRDD<Integer> rdd) {
        //路径为保存的目录，数据在目录中part-00000
        rdd.saveAsTextFile("file:///home/yushuanghe/saveAsTextFile");
    }

    private static void countByKey(JavaSparkContext sc) {
        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<>(1, 100),
                new Tuple2<>(2, 90),
                new Tuple2<>(3, 60),
                new Tuple2<>(1, 50),
                new Tuple2<>(1, 99)
        );

        JavaPairRDD<Integer, Integer> rdd = sc.parallelizePairs(scoreList, 2);

        Map<Integer, Long> map = rdd.countByKey();

        for (Map.Entry<Integer, Long> entry : map.entrySet()) {
            System.out.println(entry.getKey() + ":" + entry.getValue());
        }
    }
}